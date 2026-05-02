package io.syspulse.ika.processor.rpc3

import java.nio.file.Paths

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scala.io.Source

import akka.actor.ActorSystem
import akka.util.ByteString
import io.syspulse.ika.processor.{Session, ProcessorPipeline}
import io.syspulse.ika.processor.impl.HttpProcessor
import io.syspulse.ika.processor.ResponseSource

class SolanaProcessorSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("test")

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 3.seconds, interval = 25.millis)

  /** Project `test/` directory for Solana fixtures. */
  private val testDir = Paths.get(System.getProperty("user.dir"), "test", "rpc3", "solana")

  private def loadTestFixture(name: String): String =
    Source.fromFile(testDir.resolve(name).toFile, "UTF-8").mkString.trim

  "SolanaProcessor" should {

    "cache getSlot request" in {
      val cache = SolanaProcessor.expire(ttl = 30000L)
      val req = loadTestFixture("REQ_getSlot_finalized.json")
      val rsp = loadTestFixture("RSP_getSlot.json")
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // First call - should hit HTTP
      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.responseSource shouldBe ResponseSource.REMOTE
      httpCalls shouldBe 1

      // Second call - should be cached
      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1
    }

    "cache getBlock with commitment level" in {
      val cache = SolanaProcessor.expire(ttl = 30000L)
      val req = loadTestFixture("REQ_getBlock_finalized.json")
      val rsp = loadTestFixture("RSP_getBlock.json")
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // First call
      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.responseSource shouldBe ResponseSource.REMOTE
      httpCalls shouldBe 1

      // Second call - should be cached
      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1
    }

    "handle batch requests with getBlock" in {
      val cache = SolanaProcessor.expire(ttl = 30000L)
      val req = loadTestFixture("REQ_Batch_getBlock.json")
      val rsp = loadTestFixture("RSP_Batch_getBlock.json")
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // First call
      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.responseSource shouldBe ResponseSource.REMOTE
      httpCalls shouldBe 1

      // Second call - should be cached
      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1
    }

    "not cache error responses" in {
      val cache = SolanaProcessor.expire(ttl = 30000L)
      val rsp = """{"jsonrpc":"2.0","error":{"code":-32009,"message":"Slot was skipped"},"id":1}"""
      val req = """{"jsonrpc":"2.0","method":"getBlock","params":[123456],"id":1}"""
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // First call
      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.isRejected shouldBe false
      httpCalls shouldBe 1

      // Second call - should hit HTTP again (error not cached)
      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      httpCalls shouldBe 2
    }

    "work with no caching mode" in {
      val cache = SolanaProcessor.none()
      val req = loadTestFixture("REQ_getSlot_finalized.json")
      val rsp = loadTestFixture("RSP_getSlot.json")
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // First call
      pipeline.process(Session(requestBody = ByteString(req))).futureValue
      httpCalls shouldBe 1

      // Second call - should hit HTTP again (no caching)
      pipeline.process(Session(requestBody = ByteString(req))).futureValue
      httpCalls shouldBe 2
    }

    "handle different commitment levels" in {
      val cache = SolanaProcessor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          val rsp = """{"jsonrpc":"2.0","result":123456789,"id":1}"""
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // Test finalized commitment
      val reqFinalized = """{"jsonrpc":"2.0","method":"getSlot","params":[{"commitment":"finalized"}],"id":1}"""
      pipeline.process(Session(requestBody = ByteString(reqFinalized))).futureValue
      httpCalls shouldBe 1

      // Same request should be cached
      pipeline.process(Session(requestBody = ByteString(reqFinalized))).futureValue
      httpCalls shouldBe 1

      // Different commitment level should be a cache miss
      val reqConfirmed = """{"jsonrpc":"2.0","method":"getSlot","params":[{"commitment":"confirmed"}],"id":1}"""
      pipeline.process(Session(requestBody = ByteString(reqConfirmed))).futureValue
      httpCalls shouldBe 2

      // Same confirmed request should be cached
      pipeline.process(Session(requestBody = ByteString(reqConfirmed))).futureValue
      httpCalls shouldBe 2
    }
  }
}
