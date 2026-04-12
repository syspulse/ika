package io.syspulse.ika.processor.rpc3

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import spray.json._

import akka.actor.ActorSystem
import io.syspulse.ika.processor.{Session, ProcessorPipeline}
import io.syspulse.ika.processor.impl.HttpClientProcessor
import io.syspulse.ika.store.ProxyData

class CacheRpc3BatchSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("test")

  // Keep tests snappy
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 3.seconds, interval = 25.millis)

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
  def mockHttpProcessor()(implicit ec: ExecutionContext, actorSystem: ActorSystem): HttpClientProcessor = {
    new HttpClientProcessor(compression = "") {
      override def processRequest(session: Session): Future[Session] = {
        val req = session.requestBody
        val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
        val response = JsArray(ids.map(mkOkRes).toVector).compactPrint

        Future.successful(session.withResponse(response, ProxyData.REMOTE))
      }
    }
  }

  "CacheRpc3Processor.batch" should {

    "handle empty batch (no http calls)" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          Future.successful(session.withResponse("[]", ProxyData.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http, cache), "TestPipeline")
      val session = Session(requestBody = "[]")

      val result = pipeline.process(session).futureValue

      result.responseBody shouldBe Some("[]")
      result.responseSource shouldBe ProxyData.CACHE
      httpCalls shouldBe 0
    }

    "handle single item in batch (and cache on second call)" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = mockHttpProcessor()
      val httpCounted = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          http.process(session)
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, httpCounted, cache), "TestPipeline")
      val req = s"[${mkReq(1)}]"

      // First call - should hit HTTP
      val result1 = pipeline.process(Session(requestBody = req)).futureValue
      result1.responseSource shouldBe ProxyData.REMOTE
      httpCalls shouldBe 1

      // Second call - should be cached
      val result2 = pipeline.process(Session(requestBody = req)).futureValue
      result2.responseSource shouldBe ProxyData.CACHE
      httpCalls shouldBe 1  // Still 1, no additional HTTP call
    }

    "handle multiple items in batch (and cache on second call)" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)
      var httpCalls = 0

      val http = mockHttpProcessor()
      val httpCounted = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          httpCalls += 1
          http.process(session)
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, httpCounted, cache), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      // First call - should hit HTTP
      val result1 = pipeline.process(Session(requestBody = req)).futureValue
      result1.responseSource shouldBe ProxyData.REMOTE
      httpCalls shouldBe 1

      // Second call - should be cached
      val result2 = pipeline.process(Session(requestBody = req)).futureValue
      result2.responseSource shouldBe ProxyData.CACHE
      httpCalls shouldBe 1  // Still 1, no additional HTTP call
    }

    "fail on missing response element in batch (size mismatch)" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)

      val http = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
          // Intentionally drop the last element to emulate a broken upstream response
          val dropped = ids.dropRight(1)
          val response = JsArray(dropped.map(mkOkRes).toVector).compactPrint

          Future.successful(session.withResponse(response, ProxyData.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http, cache), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)}]"

      val result = pipeline.process(Session(requestBody = req)).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
      result.rejection.get.message should include("expected=")
    }

    "fail when the first response item is missing" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)

      val http = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
          val droppedFirst = ids.drop(1)
          val response = JsArray(droppedFirst.map(mkOkRes).toVector).compactPrint

          Future.successful(session.withResponse(response, ProxyData.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http, cache), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      val result = pipeline.process(Session(requestBody = req)).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "fail when a middle response item is missing" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)

      val http = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id")).toVector
          // Drop the middle element (index 1)
          val kept = ids.zipWithIndex.collect { case (v, i) if i != 1 => v }
          val response = JsArray(kept.map(mkOkRes)).compactPrint

          Future.successful(session.withResponse(response, ProxyData.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http, cache), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      val result = pipeline.process(Session(requestBody = req)).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "fail when the last response item is missing" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)

      val http = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
          val droppedLast = ids.dropRight(1)
          val response = JsArray(droppedLast.map(mkOkRes).toVector).compactPrint

          Future.successful(session.withResponse(response, ProxyData.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http, cache), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)}]"

      val result = pipeline.process(Session(requestBody = req)).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "fail when multiple response items are missing" in {
      val cache = CacheRpc3Processor.expire(ttl = 30000L)

      val http = new HttpClientProcessor(compression = "") {
        override def processRequest(session: Session): Future[Session] = {
          val req = session.requestBody
          val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id")).toVector
          // Keep only the first item, drop the rest
          val kept = ids.take(1)
          val response = JsArray(kept.map(mkOkRes)).compactPrint

          Future.successful(session.withResponse(response, ProxyData.REMOTE))
        }
      }

      // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http, cache), "TestPipeline")
      val req = s"[${mkReq(1)},${mkReq(2)},${mkReq(3)},${mkReq(4)}]"

      val result = pipeline.process(Session(requestBody = req)).futureValue
      result.isRejected shouldBe true
      result.rejection.get.message should include("response size")
    }

    "assemble batch from mix of cached and fresh responses (preserving order)" in {
      def runCase(allIds: Vector[Int], cachedIds: Set[Int]): Unit = {
        val cache = CacheRpc3Processor.expire(ttl = 30000L)
        var httpCalls = 0

        val http = new HttpClientProcessor(compression = "") {
          override def processRequest(session: Session): Future[Session] = {
            httpCalls += 1
            val req = session.requestBody
            val ids = req.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("id"))
            val response = JsArray(ids.map(mkOkRes).toVector).compactPrint

            Future.successful(session.withResponse(response, ProxyData.REMOTE))
          }
        }

        // Cache processor appears twice: before HTTP (request phase) and after HTTP (response phase)
      val pipeline = ProcessorPipeline.fromSeq(Seq(cache, http, cache), "TestPipeline")

        // Cache key ignores id (method+params only), so make params unique per item
        val req = allIds.map(id => mkReq(id, params = s"[${id}]")).mkString("[", ",", "]")
        val decoded = cache.decodeBatch(req).toVector

        // Seed cache for selected ids
        decoded.foreach { r =>
          val idNum = anyIdToInt(r.id)
          if (cachedIds.contains(idNum)) {
            val key = cache.getKey(r)
            val cachedRsp = mkOkRes(JsNumber(idNum)).compactPrint

            // Pre-cache by running through pipeline once
            val singleReq = s"[${mkReq(idNum, params = s"[${idNum}]")}]"
            pipeline.process(Session(requestBody = singleReq)).futureValue
          }
        }

        // Reset HTTP call counter after seeding
        httpCalls = 0

        val r1 = pipeline.process(Session(requestBody = req)).futureValue
        idsOfBatchResponse(r1.responseBody.get).map(jsIdToInt) should ===(allIds)

        if (cachedIds.size == allIds.size) httpCalls shouldBe 0
        else httpCalls shouldBe 1

        // Second call should now be fully cached
        httpCalls = 0
        val r2 = pipeline.process(Session(requestBody = req)).futureValue
        idsOfBatchResponse(r2.responseBody.get).map(jsIdToInt) should ===(allIds)
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
}
