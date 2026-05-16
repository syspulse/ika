package io.syspulse.ika.processor.core

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.RawHeader
import io.syspulse.ika.processor.{Session, ProcessorPipeline, RequestProcessor}
import io.syspulse.ika.processor.rpc3.Rpc3Processor
import io.syspulse.ika.processor.ResponseSource
import akka.util.ByteString
import io.syspulse.ika.processor.core.{CacheProcessor, HeaderProcessor, HttpProcessor, LoadBalancerStrategy, PoolProcessor, ThrottleProcessor, TimeoutProcessor}

class CoreProcessorsSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("test")
  implicit val scheduler: akka.actor.Scheduler = actorSystem.scheduler

  "ThrottleProcessor" should {
    "delay requests" in {
      val throttle = new ThrottleProcessor(50)
      val session = Session(requestBody = ByteString("test"))

      val start = System.currentTimeMillis()
      val result = Await.result(throttle.process(session), 5.seconds)
      val duration = System.currentTimeMillis() - start

      duration should be >= 50L
      result.isRejected shouldBe false
    }

    "skip throttling when delay is 0" in {
      val throttle = new ThrottleProcessor(0)
      val session = Session(requestBody = ByteString("test"))

      val start = System.currentTimeMillis()
      val result = Await.result(throttle.process(session), 5.seconds)
      val duration = System.currentTimeMillis() - start

      duration should be < 10L
      result.isRejected shouldBe false
    }
  }

  "TimeoutProcessor" should {
    "set timeout in processorData" in {
      val timeout = new TimeoutProcessor(5000)
      val session = Session(requestBody = ByteString("test"))

      val result = Await.result(timeout.process(session), 5.seconds)

      result.getData[Long]("timeoutMs") shouldBe Some(5000)
      result.isRejected shouldBe false
    }

    "set timeout and retry delay in processorData" in {
      val timeout = TimeoutProcessor(5000, 2000)
      val session = Session(requestBody = ByteString("test"))

      val result = Await.result(timeout.process(session), 5.seconds)

      result.getData[Long]("timeoutMs") shouldBe Some(5000)
      result.getData[Long]("retryDelayMs") shouldBe Some(2000)
      result.isRejected shouldBe false
    }
  }

  "HeaderProcessor" should {
    "remove and add request headers" in {
      val hp = new HeaderProcessor(
        removeRequest = Set("timeout-access", "host"),
        addRequest = Map("Content-Type" -> "application/json")
      )

      val session = Session(
        requestBody = ByteString("test"),
        requestHeaders = Seq(
          RawHeader("Host", "example.com"),
          RawHeader("Timeout-Access", "1"),
          RawHeader("X-Keep", "ok")
        )
      )

      val result = Await.result(hp.process(session), 5.seconds)

      result.requestHeaders.exists(_.is("host")) shouldBe false
      result.requestHeaders.exists(_.is("timeout-access")) shouldBe false
      result.requestHeaders.exists(_.is("x-keep")) shouldBe true

      // Content-Type is an entity attribute in Akka HTTP; it is stored in session data for HttpProcessor.
      result.requestHeaders.exists(_.is("content-type")) shouldBe false
      result.getData[String]("http.contentType") shouldBe Some("application/json")
    }
  }

  "Rpc3Processor" should {
    "cache and retrieve responses" in {
      val cacheProc = Rpc3Processor.expire(ttl = 30000L, ttlLatest = 12000L)

      val request = """{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"""
      val response = """{"jsonrpc":"2.0","result":"0x1234","id":1}"""

      var backendCalls = 0
      val backend = new RequestProcessor {
        override val name: String = "Backend"
        override def processRequest(session: Session): Future[Session] = {
          backendCalls += 1
          Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cacheProc, backend), "CacheTest")

      // First call - backend
      val r1 = Await.result(pipeline.process(Session(requestBody = ByteString(request))), 5.seconds)
      r1.responseSource shouldBe ResponseSource.REMOTE
      backendCalls shouldBe 1

      // Second call - cache
      val r2 = Await.result(pipeline.process(Session(requestBody = ByteString(request))), 5.seconds)
      r2.responseSource shouldBe ResponseSource.CACHE
      r2.responseBody.map(_.utf8String) shouldBe Some(response)
      r2.getData[Boolean]("cacheHit") shouldBe Some(true)
      backendCalls shouldBe 1
    }

    "skip caching for error responses" in {
      val cacheProc = Rpc3Processor.expire()

      val request = """{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x123"],"id":1}"""
      val errorResponse = """{"jsonrpc":"2.0","error":{"code":-32000,"message":"Error"},"id":1}"""

      var backendCalls = 0
      val backend = new RequestProcessor {
        override val name: String = "Backend"
        override def processRequest(session: Session): Future[Session] = {
          backendCalls += 1
          Future.successful(session.withResponse(ByteString(errorResponse), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cacheProc, backend), "CacheErrorTest")

      Await.result(pipeline.process(Session(requestBody = ByteString(request))), 5.seconds)
      Await.result(pipeline.process(Session(requestBody = ByteString(request))), 5.seconds)

      // Error responses are not cached => backend called twice
      backendCalls shouldBe 2
    }

    "skip caching for batch requests" in {
      val cacheProc = Rpc3Processor.expire()

      val batchRequest = """[{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}]"""
      val ok = """[{"jsonrpc":"2.0","result":"0x1","id":1}]"""

      var backendCalls = 0
      val backend = new RequestProcessor {
        override val name: String = "Backend"
        override def processRequest(session: Session): Future[Session] = {
          backendCalls += 1
          Future.successful(session.withResponse(ByteString(ok), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cacheProc, backend), "CacheBatchTest")

      val r1 = Await.result(pipeline.process(Session(requestBody = ByteString(batchRequest))), 5.seconds)
      r1.responseSource shouldBe ResponseSource.REMOTE

      val r2 = Await.result(pipeline.process(Session(requestBody = ByteString(batchRequest))), 5.seconds)
      r2.responseSource shouldBe ResponseSource.CACHE
      backendCalls shouldBe 1
    }
  }

  "PoolProcessor" should {
    "select destination from pool" in {
      val destinations = Seq("http://localhost:8545", "http://localhost:8546")
      val lb = PoolProcessor.sticky(destinations)

      val session = Session(requestBody = ByteString("test"))
      val result = Await.result(lb.process(session), 5.seconds)

      result.getData[String]("destination") should not be empty
      result.getData[String]("destination").get should (equal("http://localhost:8545") or equal("http://localhost:8546"))
      result.isRejected shouldBe false
    }

    "filter destinations by pool" in {
      val destinations = Seq("openai:https://api.openai.com/v1", "anthropic:https://api.anthropic.com/v1")
      val lb = PoolProcessor.sticky(destinations)

      val session = Session(requestBody = ByteString("test")).putData("pool", "openai")
      val result = Await.result(lb.process(session), 5.seconds)

      result.getData[String]("destination") shouldBe Some("https://api.openai.com/v1")
      result.isRejected shouldBe false
    }

    "reject when no destinations available for pool" in {
      val destinations = Seq()
      val lb = PoolProcessor.sticky(destinations)

      val session = Session(requestBody = ByteString("test")).putData("pool", "nonexistent")
      val result = Await.result(lb.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.message should include("No destinations")
    }

    "skip load balancing for cached responses" in {
      val destinations = Seq("http://localhost:8545")
      val lb = PoolProcessor.sticky(destinations)

      // Session with response already set (from cache)
      val session = Session(requestBody = ByteString("test"))
        .withResponse(ByteString("cached"), ResponseSource.CACHE)
        .putData("fromCache", true)
        .putData("cacheHit", true)

      val result = Await.result(lb.process(session), 5.seconds)

      result.getData[String]("destination") shouldBe None // Should skip
      result.responseBody.map(_.utf8String) shouldBe Some("cached")
    }

    "fail over across destinations (wrap-around) until success (sticky)" in {
      val destinations = Seq("A", "B", "C")
      val pool = PoolProcessor.sticky(destinations)

      var calls = 0
      val backend = new RequestProcessor {
        override val name: String = "Backend"
        override def processRequest(session: Session): Future[Session] = {
          calls += 1
          session.getData[String]("destination") match {
            case Some("C") => Future.successful(session.withResponse(ByteString("ok"), ResponseSource.REMOTE))
            case Some(d)   => Future.successful(session.reject(-1, s"fail:$d", name))
            case None      => Future.successful(session.reject(-1, "no-destination", name))
          }
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(pool, backend), "PoolFailoverSticky")
      val r = Await.result(pipeline.process(Session(requestBody = ByteString("test"))), 5.seconds)
      r.isRejected shouldBe false
      r.responseBody.map(_.utf8String) shouldBe Some("ok")
      calls shouldBe 3 // A, B, C
    }

    "sticky should start from last successful destination on next request" in {
      val destinations = Seq("A", "B", "C")
      val pool = PoolProcessor.sticky(destinations)

      var calls = 0
      val backend = new RequestProcessor {
        override val name: String = "Backend"
        override def processRequest(session: Session): Future[Session] = {
          calls += 1
          session.getData[String]("destination") match {
            case Some("B") => Future.successful(session.withResponse(ByteString("ok"), ResponseSource.REMOTE))
            case Some(d)   => Future.successful(session.reject(-1, s"fail:$d", name))
            case None      => Future.successful(session.reject(-1, "no-destination", name))
          }
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(pool, backend), "PoolStickyMemory")

      val r1 = Await.result(pipeline.process(Session(requestBody = ByteString("test-1"))), 5.seconds)
      r1.isRejected shouldBe false
      calls shouldBe 2 // A then B

      val r2 = Await.result(pipeline.process(Session(requestBody = ByteString("test-2"))), 5.seconds)
      r2.isRejected shouldBe false
      calls shouldBe 3 // starts at B => 1 more call
    }

    "lb should start from next after last successful destination on next request" in {
      val destinations = Seq("A", "B", "C")
      val pool = new PoolProcessor(destinations, new LoadBalancerStrategy)

      var calls = 0
      val backend = new RequestProcessor {
        override val name: String = "Backend"
        override def processRequest(session: Session): Future[Session] = {
          calls += 1
          session.getData[String]("destination") match {
            case Some("B") => Future.successful(session.withResponse(ByteString("ok"), ResponseSource.REMOTE))
            case Some(d)   => Future.successful(session.reject(-1, s"fail:$d", name))
            case None      => Future.successful(session.reject(-1, "no-destination", name))
          }
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(pool, backend), "PoolLbMemory")

      val r1 = Await.result(pipeline.process(Session(requestBody = ByteString("test-1"))), 5.seconds)
      r1.isRejected shouldBe false
      calls shouldBe 2 // A then B

      // After success at B, LB starts at C next time => C, A, B
      val r2 = Await.result(pipeline.process(Session(requestBody = ByteString("test-2"))), 5.seconds)
      r2.isRejected shouldBe false
      calls shouldBe 5 // +3 calls
    }
  }

  "HttpProcessor" should {
    "reject when no destination set" in {
      val http = new HttpProcessor()

      val session = Session(requestBody = ByteString("test"))
      val result = Await.result(http.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.processorName shouldBe "Http"
      result.rejection.get.message should include("No destination")
    }

    "skip HTTP request when response already set" in {
      val http = new HttpProcessor()

      val session = Session(requestBody = ByteString("test"))
        .withResponse(ByteString("already set"), ResponseSource.CACHE)
        .putData("destination", "http://localhost:8545")

      val result = Await.result(http.process(session), 5.seconds)

      result.responseBody.map(_.utf8String) shouldBe Some("already set")
      result.isRejected shouldBe false
    }
  }

  "Full pipeline with core processors" should {
    "process request through cache, load balancer, timeout" in {
      val destinations = Seq("http://localhost:8545")
      val cache = CacheProcessor.expire()
      val lb = PoolProcessor.sticky(destinations)
      val timeout = new TimeoutProcessor(5000)

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, timeout, lb), "TestPipeline")

      val request = """{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(pipeline.process(session), 5.seconds)

      // Should have set timeout and destination
      result.getData[Long]("timeoutMs") shouldBe Some(5000)
      result.getData[String]("destination") shouldBe Some("http://localhost:8545")
      result.getData[Boolean]("fromCache") shouldBe Some(false) // First request is cache miss
      result.isRejected shouldBe false
    }
  }
}
