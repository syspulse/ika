package io.syspulse.ika.processor

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import akka.http.scaladsl.model.headers.RawHeader
import io.syspulse.ika.store.ProxyData

class SessionSpec extends AnyWordSpec with Matchers {

  "Session" should {

    "create a basic session with request body" in {
      val session = Session(requestBody = """{"method": "eth_blockNumber"}""")

      session.requestBody shouldBe """{"method": "eth_blockNumber"}"""
      session.responseBody shouldBe None
      session.rejection shouldBe None
      session.isRejected shouldBe false
    }

    "mark session as rejected" in {
      val session = Session(requestBody = "test")
      val rejected = session.reject(400, "Invalid request", "TestProcessor")

      rejected.isRejected shouldBe true
      rejected.rejection.get.code shouldBe 400
      rejected.rejection.get.message shouldBe "Invalid request"
      rejected.rejection.get.processorName shouldBe "TestProcessor"
    }

    "store and retrieve processor data" in {
      val session = Session(requestBody = "test")
      val updated = session.putData("key1", "value1").putData("key2", 42)

      updated.getData[String]("key1") shouldBe Some("value1")
      updated.getData[Int]("key2") shouldBe Some(42)
      updated.getData[String]("missing") shouldBe None
    }

    "set response data" in {
      val session = Session(requestBody = "test")
      val headers = Seq(RawHeader("Content-Type", "application/json"))
      val updated = session.withResponse("response body", ProxyData.REMOTE, headers)

      updated.responseBody shouldBe Some("response body")
      updated.responseSource shouldBe ProxyData.REMOTE
      updated.responseHeaders shouldBe headers
    }

    "store destination URL in processorData" in {
      val session = Session(requestBody = "test")
      val updated = session.putData("destination", "http://localhost:8545")

      updated.getData[String]("destination") shouldBe Some("http://localhost:8545")
    }

    "store pool name in processorData" in {
      val session = Session(requestBody = "test")
      val updated = session.putData("pool", "openai")

      updated.getData[String]("pool") shouldBe Some("openai")
    }

    "store cache hit in processorData" in {
      val session = Session(requestBody = "test")
      val updated = session
        .putData("cacheHit", true)
        .withResponse("cached", ProxyData.CACHE)

      updated.getData[Boolean]("cacheHit") shouldBe Some(true)
      updated.responseSource shouldBe ProxyData.CACHE
    }

    "store retry counter in processorData" in {
      val session = Session(requestBody = "test")
      val retry1 = session.putData("retry", 1)
      val retry2 = retry1.putData("retry", 2)

      retry2.getData[Int]("retry") shouldBe Some(2)
    }

    "track duration" in {
      val session = Session(requestBody = "test")
      Thread.sleep(10)
      val completed = session.complete()

      completed.durationMs should be >= 10L
      completed.endTime shouldBe defined
    }

    "convert to ProxyData" in {
      val session = Session(
        requestBody = "test",
        responseBody = Some("response"),
        responseSource = ProxyData.CACHE
      )

      val proxyData = session.toProxyData
      proxyData.body shouldBe "response"
      proxyData.src shouldBe ProxyData.CACHE
    }

    "be immutable - updates create new instances" in {
      val original = Session(requestBody = "test")
      val updated = original.putData("destination", "http://localhost:8545")

      original.getData[String]("destination") shouldBe None
      updated.getData[String]("destination") shouldBe Some("http://localhost:8545")
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
