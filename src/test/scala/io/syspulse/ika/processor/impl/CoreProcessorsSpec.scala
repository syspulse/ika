package io.syspulse.ika.processor.impl

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import io.syspulse.ika.processor.{Session, ProcessorPipeline}
import io.syspulse.ika.processor.rpc3.CacheRpc3Processor
import io.syspulse.ika.store.ProxyData

class CoreProcessorsSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("test")
  implicit val scheduler: akka.actor.Scheduler = actorSystem.scheduler

  "ThrottleProcessor" should {
    "delay requests" in {
      val throttle = new ThrottleProcessor(50, global = false)
      val session = Session(requestBody = "test")

      val start = System.currentTimeMillis()
      val result = Await.result(throttle.process(session), 5.seconds)
      val duration = System.currentTimeMillis() - start

      duration should be >= 50L
      result.isRejected shouldBe false
    }

    "skip throttling when delay is 0" in {
      val throttle = new ThrottleProcessor(0)
      val session = Session(requestBody = "test")

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
      val session = Session(requestBody = "test")

      val result = Await.result(timeout.process(session), 5.seconds)

      result.getData[Long]("timeoutMs") shouldBe Some(5000)
      result.isRejected shouldBe false
    }

    "set timeout and retry delay in processorData" in {
      val timeout = TimeoutProcessor(5000, 2000)
      val session = Session(requestBody = "test")

      val result = Await.result(timeout.process(session), 5.seconds)

      result.getData[Long]("timeoutMs") shouldBe Some(5000)
      result.getData[Long]("retryDelayMs") shouldBe Some(2000)
      result.isRejected shouldBe false
    }
  }

  "CacheProcessor" should {
    "cache and retrieve responses" in {
      val cacheProc = CacheRpc3Processor.expire(ttl = 30000L, ttlLatest = 12000L)

      val request = """{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"""
      val response = """{"jsonrpc":"2.0","result":"0x1234","id":1}"""

      // First request - cache miss (request phase)
      val session1 = Session(requestBody = request)
      val afterReq1 = Await.result(cacheProc.process(session1), 5.seconds)
      afterReq1.getData[Boolean]("fromCache") shouldBe Some(false)
      afterReq1.responseBody shouldBe None

      // Simulate downstream setting response
      val withResponse = afterReq1.withResponse(response, ProxyData.REMOTE)

      // Response phase - cache the response
      val afterResp1 = Await.result(cacheProc.process(withResponse), 5.seconds)

      // Second request - cache hit
      val session2 = Session(requestBody = request)
      val afterReq2 = Await.result(cacheProc.process(session2), 5.seconds)

      afterReq2.getData[Boolean]("fromCache") shouldBe Some(true)
      afterReq2.responseBody shouldBe Some(response)
      afterReq2.responseSource shouldBe ProxyData.CACHE
      afterReq2.getData[Boolean]("cacheHit") shouldBe Some(true)
    }

    "skip caching for error responses" in {
      val cacheProc = CacheRpc3Processor.expire()

      val request = """{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x123"],"id":1}"""
      val errorResponse = """{"jsonrpc":"2.0","error":{"code":-32000,"message":"Error"},"id":1}"""

      // Request phase
      val session = Session(requestBody = request)
      val afterReq = Await.result(cacheProc.process(session), 5.seconds)

      // Simulate response with error
      val withResponse = afterReq.withResponse(errorResponse, ProxyData.REMOTE)
      val afterResp = Await.result(cacheProc.process(withResponse), 5.seconds)

      // Try to retrieve - should be cache miss (error wasn't cached)
      val session2 = Session(requestBody = request)
      val afterReq2 = Await.result(cacheProc.process(session2), 5.seconds)
      afterReq2.getData[Boolean]("fromCache") shouldBe Some(false)
    }

    "skip caching for batch requests" in {
      val cacheProc = CacheRpc3Processor.expire()

      val batchRequest = """[{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}]"""

      val session = Session(requestBody = batchRequest)
      val result = Await.result(cacheProc.process(session), 5.seconds)

      result.responseBody shouldBe None // Should not process batch requests
    }
  }

  "LoadBalancerProcessor" should {
    "select destination from pool" in {
      val destinations = Seq("http://localhost:8545", "http://localhost:8546")
      val lb = LoadBalancerProcessor.sticky(destinations)

      val session = Session(requestBody = "test")
      val result = Await.result(lb.process(session), 5.seconds)

      result.getData[String]("destination") should not be empty
      result.getData[String]("destination").get should (equal("http://localhost:8545") or equal("http://localhost:8546"))
      result.isRejected shouldBe false
    }

    "filter destinations by pool" in {
      val destinations = Seq("openai:https://api.openai.com/v1", "anthropic:https://api.anthropic.com/v1")
      val lb = LoadBalancerProcessor.sticky(destinations)

      val session = Session(requestBody = "test").putData("pool", "openai")
      val result = Await.result(lb.process(session), 5.seconds)

      result.getData[String]("destination") shouldBe Some("openai:https://api.openai.com/v1")
      result.isRejected shouldBe false
    }

    "reject when no destinations available for pool" in {
      val destinations = Seq()
      val lb = LoadBalancerProcessor.sticky(destinations)

      val session = Session(requestBody = "test").putData("pool", "nonexistent")
      val result = Await.result(lb.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.message should include("No destinations")
    }

    "skip load balancing for cached responses" in {
      val destinations = Seq("http://localhost:8545")
      val lb = LoadBalancerProcessor.sticky(destinations)

      // Session with response already set (from cache)
      val session = Session(requestBody = "test")
        .withResponse("cached", ProxyData.CACHE)
        .putData("fromCache", true)
        .putData("cacheHit", true)

      val result = Await.result(lb.process(session), 5.seconds)

      result.getData[String]("destination") shouldBe None // Should skip
      result.responseBody shouldBe Some("cached")
    }
  }

  "HttpClientProcessor" should {
    "reject when no destination set" in {
      val http = HttpClientProcessor("")

      val session = Session(requestBody = "test")
      val result = Await.result(http.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.processorName shouldBe "HttpClient"
      result.rejection.get.message should include("No destination")
    }

    "skip HTTP request when response already set" in {
      val http = HttpClientProcessor("")

      val session = Session(requestBody = "test")
        .withResponse("already set", ProxyData.CACHE)
        .putData("destination", "http://localhost:8545")

      val result = Await.result(http.process(session), 5.seconds)

      result.responseBody shouldBe Some("already set")
      result.isRejected shouldBe false
    }
  }

  "Full pipeline with core processors" should {
    "process request through cache, load balancer, timeout" in {
      val destinations = Seq("http://localhost:8545")
      val cache = CacheRpc3Processor.expire()
      val lb = LoadBalancerProcessor.sticky(destinations)
      val timeout = new TimeoutProcessor(5000)

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, timeout, lb), "TestPipeline")

      val request = """{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"""
      val session = Session(requestBody = request)

      val result = Await.result(pipeline.process(session), 5.seconds)

      // Should have set timeout and destination
      result.getData[Long]("timeoutMs") shouldBe Some(5000)
      result.getData[String]("destination") shouldBe Some("http://localhost:8545")
      result.getData[Boolean]("fromCache") shouldBe Some(false) // First request is cache miss
      result.isRejected shouldBe false
    }
  }
}
