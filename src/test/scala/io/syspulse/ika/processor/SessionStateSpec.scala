package io.syspulse.ika.processor

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._

import akka.actor.ActorSystem
import io.syspulse.ika.processor.impl.CacheProcessor
import io.syspulse.ika.processor.rpc3.CacheRpc3Processor
import io.syspulse.ika.store.ProxyData

class SessionStateSpec extends AnyWordSpec with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("test")

  "Session state management" should {
    "start with CONTINUE state by default" in {
      val session = Session(requestBody = "test")

      session.state shouldBe SessionState.CONTINUE
      session.shouldContinue shouldBe true
      session.shouldReturn shouldBe false
      session.isRejected shouldBe false
    }

    "transition to REJECT state on rejection" in {
      val session = Session(requestBody = "test")
        .reject(code = -32600, message = "Invalid request", processorName = "TestProcessor")

      session.state shouldBe SessionState.REJECT
      session.isRejected shouldBe true
      session.shouldContinue shouldBe false
      session.shouldReturn shouldBe false
    }

    "transition to RETURN state on early return" in {
      val session = Session(requestBody = "test")
        .returnEarly("cache_hit")

      session.state shouldBe SessionState.RETURN
      session.shouldReturn shouldBe true
      session.shouldContinue shouldBe false
      session.isRejected shouldBe false
      session.getData[String]("returnReason") shouldBe Some("cache_hit")
    }
  }

  "ProcessorPipeline with state" should {
    "skip remaining processors on REJECT" in {
      var processor2Called = false
      var processor3Called = false

      val processor1 = new Processor {
        def name = "Processor1"
        def process(session: Session): Future[Session] = {
          Future.successful(session.reject(-32600, "Error", "Processor1"))
        }
      }

      val processor2 = new Processor {
        def name = "Processor2"
        def process(session: Session): Future[Session] = {
          processor2Called = true
          Future.successful(session)
        }
      }

      val processor3 = new Processor {
        def name = "Processor3"
        def process(session: Session): Future[Session] = {
          processor3Called = true
          Future.successful(session)
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(processor1, processor2, processor3), "TestPipeline")
      val session = Session(requestBody = "test")

      val result = Await.result(pipeline.process(session), 5.seconds)

      result.isRejected shouldBe true
      processor2Called shouldBe false
      processor3Called shouldBe false
    }

    "skip remaining processors on RETURN (early return)" in {
      var processor2Called = false
      var processor3Called = false

      val processor1 = new Processor {
        def name = "Processor1"
        def process(session: Session): Future[Session] = {
          Future.successful(
            session
              .withResponseBody("cached response")
              .returnEarly("cache_hit")
          )
        }
      }

      val processor2 = new Processor {
        def name = "Processor2"
        def process(session: Session): Future[Session] = {
          processor2Called = true
          Future.successful(session)
        }
      }

      val processor3 = new Processor {
        def name = "Processor3"
        def process(session: Session): Future[Session] = {
          processor3Called = true
          Future.successful(session)
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(processor1, processor2, processor3), "TestPipeline")
      val session = Session(requestBody = "test")

      val result = Await.result(pipeline.process(session), 5.seconds)

      result.shouldReturn shouldBe true
      result.responseBody shouldBe Some("cached response")
      processor2Called shouldBe false
      processor3Called shouldBe false
    }

    "execute all processors on CONTINUE" in {
      var processor1Called = false
      var processor2Called = false
      var processor3Called = false

      val processor1 = new Processor {
        def name = "Processor1"
        def process(session: Session): Future[Session] = {
          processor1Called = true
          Future.successful(session)
        }
      }

      val processor2 = new Processor {
        def name = "Processor2"
        def process(session: Session): Future[Session] = {
          processor2Called = true
          Future.successful(session)
        }
      }

      val processor3 = new Processor {
        def name = "Processor3"
        def process(session: Session): Future[Session] = {
          processor3Called = true
          Future.successful(session)
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(processor1, processor2, processor3), "TestPipeline")
      val session = Session(requestBody = "test")

      val result = Await.result(pipeline.process(session), 5.seconds)

      result.shouldContinue shouldBe true
      processor1Called shouldBe true
      processor2Called shouldBe true
      processor3Called shouldBe true
    }
  }

  "CacheProcessor with state" should {
    "return early (RETURN state) on cache hit" in {
      var httpClientCalled = false

      val cache = CacheRpc3Processor.expire(ttl = 30000L)
      val httpClient = new Processor {
        def name = "HttpClient"
        def process(session: Session): Future[Session] = {
          httpClientCalled = true
          Future.successful(session.withResponseBody("from http"))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, httpClient), "CachePipeline")

      val request = """{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"""
      val response = """{"jsonrpc":"2.0","result":"0x1234","id":1}"""

      // First request - cache miss, should call HTTP
      val session1 = Session(requestBody = request)
      val result1 = Await.result(pipeline.process(session1), 5.seconds)

      httpClientCalled shouldBe true
      result1.responseBody shouldBe Some("from http")
      result1.shouldContinue shouldBe true

      // Cache the response manually
      val session1WithCache = result1.withResponseBody(response)
      Await.result(cache.process(session1WithCache), 5.seconds)

      // Second request - cache hit, should NOT call HTTP
      httpClientCalled = false
      val session2 = Session(requestBody = request)
      val result2 = Await.result(pipeline.process(session2), 5.seconds)

      httpClientCalled shouldBe false  // HTTP should NOT be called
      result2.shouldReturn shouldBe true  // Should return early
      result2.responseBody shouldBe Some(response)  // Should have cached response
      result2.responseSource shouldBe ProxyData.CACHE
      result2.getData[String]("returnReason") shouldBe Some("cache_hit")
    }
  }
}
