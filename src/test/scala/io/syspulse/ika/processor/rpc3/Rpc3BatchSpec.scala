package io.syspulse.ika.processor.rpc3

import java.nio.file.Paths

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scala.io.Source
import spray.json._

import akka.actor.ActorSystem
import io.syspulse.ika.processor.{Session, ProcessorPipeline}
import io.syspulse.ika.processor.core.HttpProcessor
import io.syspulse.ika.processor.ResponseSource
import akka.util.ByteString

class Rpc3BatchSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("test")

  // Keep tests snappy
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 3.seconds, interval = 25.millis)

  /** Project `test/` directory (see `build.sbt` / `-Duser.dir` for runs). */
  private val testDir = Paths.get(System.getProperty("user.dir"), "test", "rpc3", "evm")

  private def loadTestFixture(name: String): String =
    Source.fromFile(testDir.resolve(name).toFile, "UTF-8").mkString.trim

  /** Canonical JSON string for stable equality on real RPC fixtures. */
  private def jsonCanon(s: String): String =
    s.parseJson.compactPrint

  def mkReq(id: Int, method: String = "eth_test", params: String = "[]"): String =
    s"""{"jsonrpc":"2.0","method":"${method}","params":${params},"id":${id}}"""

  def mkOkRes(id: JsValue): JsValue =
    JsObject(
      "jsonrpc" -> JsString("2.0"),
      "id" -> id,
      "result" -> JsString(s"ok-${id.compactPrint}")
    )

  private def idsOfBatchResponse(body: String): Vector[JsValue] =
    body.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))

  private def jsIdToInt(id: JsValue): Int =
    id match {
      case JsNumber(n) => n.toIntExact
      case JsString(s) => s.toInt
      case other       => throw new RuntimeException(s"unsupported id json: ${other}")
    }

  private def anyIdToInt(id: Any): Int =
    id match {
      case i: Int    => i
      case l: Long   => l.toInt
      case d: Double => d.toInt
      case bd: BigDecimal => bd.toInt
      case s: String => s.toInt
      case other     => throw new RuntimeException(s"unsupported id type: ${other.getClass.getName}: ${other}")
    }

  /**
   * Create a mock HTTP processor that responds with OK for each request ID
   */
  def mockHttpProcessor()(implicit ec: ExecutionContext, actorSystem: ActorSystem): HttpProcessor = {
    new HttpProcessor(compression = "") {
      override def processRequest(session: Session): Future[Session] = {
        val req = session.requestBody.utf8String
        val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
        val response = JsArray(ids.map(mkOkRes).toVector).compactPrint

        Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
      }
    }
  }

  "Rpc3Processor.batch" should {

    "handle empty batch (no http calls)" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString("[]"), ResponseSource.REMOTE))
        }
      }

      // Cache processor calls downstream itself (request+response in one pass)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")
      val session = Session(requestBody = ByteString("[]"))

      val result = pipeline.process(session).futureValue

      result.responseBody.map(_.utf8String) shouldBe Some("[]")
      result.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 0
    }

    "handle single item in batch (and cache on second call)" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = mockHttpProcessor()
      val httpCounted = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          http.process(session)
        }
      }

      // Cache processor calls downstream itself (request+response in one pass)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, httpCounted), "TestPipeline")
      val req = s"[${mkReq(1)}]"

      // First call - should hit HTTP
      val result1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result1.responseSource shouldBe ResponseSource.REMOTE
      httpCalls shouldBe 1

      // Second call - should be cached
      val result2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1  // Still 1, no additional HTTP call
    }

    "handle multiple items in batch (and cache on second call)" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = mockHttpProcessor()
      val httpCounted = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          http.process(session)
        }
      }

      // Cache processor calls downstream itself (request+response in one pass)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, httpCounted), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      // First call - should hit HTTP
      val result1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result1.responseSource shouldBe ResponseSource.REMOTE
      httpCalls shouldBe 1

      // Second call - should be cached
      val result2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1  // Still 1, no additional HTTP call
    }

    "fail on missing response element in batch (size mismatch)" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody.utf8String
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
          // Intentionally drop the last element to emulate a broken upstream response
          val dropped = ids.dropRight(1)
          val response = JsArray(dropped.map(mkOkRes).toVector).compactPrint

          Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
        }
      }

      // Cache processor calls downstream itself (request+response in one pass)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)}]"

      val result = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
      result.rejection.get.message should include("expected=")
    }

    "fail when the first response item is missing" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody.utf8String
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
          val droppedFirst = ids.drop(1)
          val response = JsArray(droppedFirst.map(mkOkRes).toVector).compactPrint

          Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      val result = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "fail when a middle response item is missing" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody.utf8String
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id")).toVector
          // Drop the middle element (index 1)
          val kept = ids.zipWithIndex.collect { case (v, i) if i != 1 => v }
          val response = JsArray(kept.map(mkOkRes)).compactPrint

          Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      val result = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "fail when the last response item is missing" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody.utf8String
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
          val droppedLast = ids.dropRight(1)
          val response = JsArray(droppedLast.map(mkOkRes).toVector).compactPrint

          Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      val result = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "fail when multiple response items are missing" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody.utf8String
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id")).toVector
          // Keep only the first item, drop the rest
          val kept = ids.take(1)
          val response = JsArray(kept.map(mkOkRes)).compactPrint

          Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)},${mkReq(4)}]"

      val result = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "assemble batch from mix of cached and fresh responses (preserving order)" in {
      def runCase(allIds: Vector[Int], cachedIds: Set[Int]): Unit = {
        val cache = Rpc3Processor.expire(ttl = 30000L)
        var httpCalls = 0

        val http = new HttpProcessor(compression = "") {
          override def processRequest(session: Session): Future[Session] = {
            httpCalls += 1
            val req = session.requestBody.utf8String
            val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
            val response = JsArray(ids.map(mkOkRes).toVector).compactPrint

            Future.successful(session.withResponse(ByteString(response), ResponseSource.REMOTE))
          }
        }

        // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

        // Cache key ignores id (method+params only), so make params unique per item
        val req = allIds.map(id => mkReq(id, params = s"[${id}]")).mkString("[", ",", "]")
        val decoded = cache.decodeBatch(req).toVector

        // Seed cache for selected ids
        decoded.foreach { case (r, jsonStr) =>
          val idNum = anyIdToInt(r.id)
          if (cachedIds.contains(idNum)) {
            val key = cache.getKey(r)
            val cachedRsp = mkOkRes(JsNumber(idNum)).compactPrint

            // Pre-cache by running through pipeline once
            val singleReq = s"[${mkReq(idNum, params = s"[${idNum}]")}]"
            pipeline.process(Session(requestBody = ByteString(singleReq))).futureValue
          }
        }

        // Reset HTTP call counter after seeding
        httpCalls = 0

        val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
        idsOfBatchResponse(r1.responseBody.get.utf8String).map(jsIdToInt) should ===(allIds)

        if (cachedIds.size == allIds.size) httpCalls shouldBe 0
        else httpCalls shouldBe 1

        // Second call should now be fully cached
        httpCalls = 0
        val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
        idsOfBatchResponse(r2.responseBody.get.utf8String).map(jsIdToInt) should ===(allIds)
        httpCalls shouldBe 0
      }

      // Cached in the beginning
      runCase(allIds = Vector(1, 2, 3, 4), cachedIds = Set(1, 2))
      // Cached in the middle (order should remain 1,2,3,4,5)
      runCase(allIds = Vector(1, 2, 3, 4, 5), cachedIds = Set(3))
      // Alternating cache/fresh
      runCase(allIds = Vector(10, 11, 12, 13, 14, 15), cachedIds = Set(10, 12, 14))
      // All cached (no http)
      runCase(allIds = Vector(21, 22), cachedIds = Set(21, 22))
    }
  }

  /**
   * Real captured JSON-RPC responses under `test/RSP*.json`, paired with matching requests.
   * Bodies are compared after `parseJson.compactPrint` so formatting differences do not matter.
   */
  "Rpc3Processor (test/ RSP*.json fixtures)" should {

    "cache single eth_getBlockByNumber(latest) using REQ_latest.json + RSP_1.json" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      val rsp = loadTestFixture("RSP_1.json")
      val req = loadTestFixture("REQ_latest.json")
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.responseSource shouldBe ResponseSource.REMOTE
      jsonCanon(r1.responseBody.get.utf8String) shouldBe jsonCanon(rsp)
      httpCalls shouldBe 1

      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      jsonCanon(r2.responseBody.get.utf8String) shouldBe jsonCanon(rsp)
      httpCalls shouldBe 1
    }

    "cache eth_blockNumber using REQ_eth_blockNumber_latest.json + RSP_eth_blockNumber.json" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      val rsp = loadTestFixture("RSP_eth_blockNumber.json")
      val req = loadTestFixture("REQ_eth_blockNumber_latest.json")
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.responseSource shouldBe ResponseSource.REMOTE
      jsonCanon(r1.responseBody.get.utf8String) shouldBe jsonCanon(rsp)
      httpCalls shouldBe 1

      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1
    }

    "cache batch item using RSP_Batch_1.json (request id aligned with fixture)" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      val rsp = loadTestFixture("RSP_Batch_1.json")
      val req =
        """[{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest", false],"id":1}]"""
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.responseSource shouldBe ResponseSource.REMOTE
      jsonCanon(r1.responseBody.get.utf8String) shouldBe jsonCanon(rsp)
      httpCalls shouldBe 1

      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1
    }

    "cache batch item using RSP_Batch_2.json (id 2)" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      val rsp = loadTestFixture("RSP_Batch_2.json")
      val req =
        """[{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest", false],"id":2}]"""
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      jsonCanon(r1.responseBody.get.utf8String) shouldBe jsonCanon(rsp)
      httpCalls shouldBe 1

      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r2.responseSource shouldBe ResponseSource.CACHE
      httpCalls shouldBe 1
    }

    "not cache single error response (RSP_error_32000.json); second request hits HTTP again" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
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

      val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      r1.isRejected shouldBe false
      jsonCanon(r1.responseBody.get.utf8String) shouldBe jsonCanon(rsp)
      httpCalls shouldBe 1

      val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
      jsonCanon(r2.responseBody.get.utf8String) shouldBe jsonCanon(rsp)
      httpCalls shouldBe 2
    }

    "not cache batch error response (RSP_Batch_error_32000.json); second request hits HTTP again" in {
      val cache = Rpc3Processor.expire(ttl = 30000L)
      val rsp = loadTestFixture("RSP_Batch_error_32000.json")
      val req = """[{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":0}]"""
      var httpCalls = 0

      val http = new HttpProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse(ByteString(rsp), ResponseSource.REMOTE))
        }
      }

      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http), "TestPipeline")

      pipeline.process(Session(requestBody = ByteString(req))).futureValue
      httpCalls shouldBe 1

      pipeline.process(Session(requestBody = ByteString(req))).futureValue
      httpCalls shouldBe 2
    }
  }
}
