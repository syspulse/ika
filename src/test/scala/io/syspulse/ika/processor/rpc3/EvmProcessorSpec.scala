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

class EvmProcessorSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("test")

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 3.seconds, interval = 25.millis)

  /** Project `test/` directory. */
  private val testDir = Paths.get(System.getProperty("user.dir"), "test", "rpc3", "evm")

  private def loadTestFixture(name: String): String =
    Source.fromFile(testDir.resolve(name).toFile, "UTF-8").mkString.trim

  "EvmProcessor" should {

    "cache eth_blockNumber request" in {
      val cache = EvmProcessor.expire(ttl = 30000L)
      val req = loadTestFixture("REQ_eth_blockNumber_latest.json")
      val rsp = loadTestFixture("RSP_eth_blockNumber.json")
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

    "cache eth_getBlockByNumber with 'latest'" in {
      val cache = EvmProcessor.expire(ttl = 30000L)
      val req = loadTestFixture("REQ_latest.json")
      val rsp = loadTestFixture("RSP_latest.json")
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // First call with "latest"
      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.responseSource shouldBe ResponseSource.REMOTE
      httpCalls shouldBe 1

      // Second call with "latest" - should be cached
      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1
    }

    "handle batch requests with eth_getBlockByNumber" in {
      val cache = EvmProcessor.expire(ttl = 30000L)
      val req = loadTestFixture("REQ_Batch_latest.json")
      val rsp = loadTestFixture("RSP_Batch_1.json")
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
      val cache = EvmProcessor.expire(ttl = 30000L)
      val rsp = loadTestFixture("RSP_error_32000.json")
      val req = """{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":0}"""
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
      val cache = EvmProcessor.none()
      val req = loadTestFixture("REQ_latest.json")
      val rsp = loadTestFixture("RSP_latest.json")
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

    "cache different EVM methods independently" in {
      val cache = EvmProcessor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          val rsp = """{"jsonrpc":"2.0","result":"0x123abc","id":1}"""
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      // Test eth_blockNumber
      val reqBlockNumber = """{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"""
      pipeline.process(Session(requestBody = ByteString(reqBlockNumber))).futureValue
      httpCalls shouldBe 1

      // Same request should be cached
      pipeline.process(Session(requestBody = ByteString(reqBlockNumber))).futureValue
      httpCalls shouldBe 1

      // Different method should be a cache miss
      val reqChainId = """{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}"""
      pipeline.process(Session(requestBody = ByteString(reqChainId))).futureValue
      httpCalls shouldBe 2

      // Same chainId request should be cached
      pipeline.process(Session(requestBody = ByteString(reqChainId))).futureValue
      httpCalls shouldBe 2
    }
  }
}
