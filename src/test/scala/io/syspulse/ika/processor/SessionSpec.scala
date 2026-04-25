package io.syspulse.ika.processor

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import akka.http.scaladsl.model.headers.RawHeader
import io.syspulse.ika.processor.ResponseSource
import akka.util.ByteString

class SessionSpec extends AnyWordSpec with Matchers {

  "Session" should {

    "create a basic session with request body" in {
      val session = Session(requestBody = ByteString("""{"method": "eth_blockNumber"}"""))

      session.requestBody.utf8String shouldBe """{"method": "eth_blockNumber"}"""
      session.responseBody shouldBe None
      session.rejection shouldBe None
      session.isRejected shouldBe false
    }

    "mark session as rejected" in {
      val session = Session(requestBody = ByteString("test"))
      val rejected = session.reject(400, "Invalid request", "TestProcessor")

      rejected.isRejected shouldBe true
      rejected.rejection.get.code shouldBe 400
      rejected.rejection.get.message shouldBe "Invalid request"
      rejected.rejection.get.processorName shouldBe "TestProcessor"
    }

    "store and retrieve processor data" in {
      val session = Session(requestBody = ByteString("test"))
      val updated = session.putData("key1", "value1").putData("key2", 42)

      updated.getData[String]("key1") shouldBe Some("value1")
      updated.getData[Int]("key2") shouldBe Some(42)
      updated.getData[String]("missing") shouldBe None
    }

    "set response data" in {
      val session = Session(requestBody = ByteString("test"))
      val headers = Seq(RawHeader("Content-Type", "application/json"))
      val updated = session.withResponse(ByteString("response body"), ResponseSource.REMOTE, headers = headers)

      updated.responseBody.map(_.utf8String) shouldBe Some("response body")
      updated.responseSource shouldBe ResponseSource.REMOTE
      updated.responseHeaders shouldBe headers
    }

    "update request body immutably for upstream/downstream processors" in {
      val session = Session(requestBody = ByteString("""{"id":1}"""))
      val rewritten = session.withRequestBody(ByteString("""{"id":2}"""))

      session.requestBody.utf8String shouldBe """{"id":1}"""
      rewritten.requestBody.utf8String shouldBe """{"id":2}"""
    }

    "replace request headers with withRequestHeaders" in {
      val h1 = RawHeader("A", "1")
      val h2 = RawHeader("B", "2")
      val session = Session(requestBody = ByteString("x"), requestHeaders = Seq(h1))
      val replaced = session.withRequestHeaders(Seq(h2))

      session.requestHeaders shouldBe Seq(h1)
      replaced.requestHeaders shouldBe Seq(h2)
    }

    "append request headers with addRequestHeader" in {
      val session = Session(requestBody = ByteString("x"), requestHeaders = Seq(RawHeader("A", "1")))
      val added = session.addRequestHeader(RawHeader("B", "2"))

      added.requestHeaders.map(h => (h.name, h.value)).toSet shouldBe Set(("A", "1"), ("B", "2"))
    }

    "deduplicate request headers on construction (case-insensitive names; last wins)" in {
      val s = Session(
        requestBody = ByteString("x"),
        requestHeaders = Seq(
          RawHeader("X-Token", "first"),
          RawHeader("Other", "o"),
          RawHeader("x-token", "second")
        )
      )
      s.requestHeaderMap.size shouldBe 2
      s.requestHeaderMap("other").value shouldBe "o"
      s.requestHeaderMap("x-token").value shouldBe "second"
    }

    "replace request header when addRequestHeader uses the same field name (case-insensitive)" in {
      val s0 = Session(requestBody = ByteString("x"), requestHeaders = Seq(RawHeader("Authorization", "OLD")))
      val s1 = s0.addRequestHeader(RawHeader("authorization", "NEW"))
      s1.requestHeaders should have length 1
      s1.requestHeaders.head.value shouldBe "NEW"
    }

    "deduplicate response headers in withResponse" in {
      val s = Session(requestBody = ByteString("x")).withResponse(
        ByteString("{}"),
        ResponseSource.REMOTE,
        headers = Seq(RawHeader("Vary", "*"), RawHeader("vary", "Origin"))
      )
      s.responseHeaders should have length 1
      s.responseHeaders.head.value shouldBe "Origin"
    }

    "store destination URL in processorData" in {
      val session = Session(requestBody = ByteString("test"))
      val updated = session.putData("destination", "http://localhost:8545")

      updated.getData[String]("destination") shouldBe Some("http://localhost:8545")
    }

    "store pool name in processorData" in {
      val session = Session(requestBody = ByteString("test"))
      val updated = session.putData("pool", "openai")

      updated.getData[String]("pool") shouldBe Some("openai")
    }

    "store cache hit in processorData" in {
      val session = Session(requestBody = ByteString("test"))
      val updated = session
        .putData("cacheHit", true)
        .withResponse(ByteString("cached"), ResponseSource.CACHE)

      updated.getData[Boolean]("cacheHit") shouldBe Some(true)
      updated.responseSource shouldBe ResponseSource.CACHE
    }

    "store retry counter in processorData" in {
      val session = Session(requestBody = ByteString("test"))
      val retry1 = session.putData("retry", 1)
      val retry2 = retry1.putData("retry", 2)

      retry2.getData[Int]("retry") shouldBe Some(2)
    }

    "track duration" in {
      val session = Session(requestBody = ByteString("test"))
      Thread.sleep(10)
      val completed = session.complete()

      completed.durationMs should be >= 10L
      completed.endTime shouldBe defined
    }

    "be immutable - updates create new instances" in {
      val original = Session(requestBody = ByteString("test"))
      val updated = original.putData("destination", "http://localhost:8545")

      original.getData[String]("destination") shouldBe None
      updated.getData[String]("destination") shouldBe Some("http://localhost:8545")
    }
  }

  "Session.headersFromSeq" should {
    "map duplicate field names to last value (case-insensitive key)" in {
      val m = Session.headersFromSeq(
        Seq(
          RawHeader("X-Req", "a"),
          RawHeader("Y", "y"),
          RawHeader("x-req", "b")
        )
      )
      m.size shouldBe 2
      m("x-req").value shouldBe "b"
      m("y").value shouldBe "y"
    }
  }

  "Rejection" should {

    "store error information" in {
      val rejection = Rejection(
        code = -32601,
        message = "Method not found",
        processorName = "TestProcessor"
      )

      rejection.code shouldBe -32601
      rejection.message shouldBe "Method not found"
      rejection.processorName shouldBe "TestProcessor"
      rejection.details shouldBe None
    }

    "store optional details" in {
      val rejection = Rejection(
        code = -32603,
        message = "Internal error",
        processorName = "TestProcessor",
        details = Some("Connection timeout")
      )

      rejection.details shouldBe Some("Connection timeout")
    }
  }
}
