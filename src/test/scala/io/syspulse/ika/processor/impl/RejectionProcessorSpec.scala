package io.syspulse.ika.processor.impl

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._

import io.syspulse.ika.processor.{Session, Rejection}
import io.syspulse.ika.processor.ResponseSource

class RejectionProcessorSpec extends AnyWordSpec with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.global

  "JsonRpcRejectionProcessor" should {

    "convert rejection to JSON-RPC 2.0 format" in {
      val processor = RejectionProcessor.jsonRpc()
      val rejection = Rejection(
        code = -32601,
        message = "Method not found",
        processorName = "TestProcessor"
      )
      val session = Session(requestBody = "test").copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      result.responseBody shouldBe defined
      val body = result.responseBody.get
      body should include(""""jsonrpc": "2.0"""")
      body should include(""""code": -32601""")
      body should include(""""message": "Method not found"""")
      body should include(""""processor": "TestProcessor"""")
    }

    "include details in error data when provided" in {
      val processor = RejectionProcessor.jsonRpc(includeDetails = true)
      val rejection = Rejection(
        code = -32603,
        message = "Internal error",
        processorName = "HttpClient",
        details = Some("Connection timeout")
      )
      val session = Session(requestBody = "test").copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      val body = result.responseBody.get
      body should include(""""details": "Connection timeout"""")
    }

    "exclude processor name when configured" in {
      val processor = RejectionProcessor.jsonRpc(includeProcessor = false)
      val rejection = Rejection(
        code = -32603,
        message = "Internal error",
        processorName = "TestProcessor"
      )
      val session = Session(requestBody = "test").copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      val body = result.responseBody.get
      body should not include "processor"
    }

    "set HTTP status code to 200 by default" in {
      val processor = RejectionProcessor.jsonRpc()
      val rejection = Rejection(-32603, "Error", "Test")
      val session = Session(requestBody = "test").copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      result.getData[Int]("httpStatusCode") shouldBe Some(200)
    }

    "allow custom HTTP status code" in {
      val processor = RejectionProcessor.jsonRpc(httpStatusCode = 500)
      val rejection = Rejection(-32603, "Error", "Test")
      val session = Session(requestBody = "test").copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      result.getData[Int]("httpStatusCode") shouldBe Some(500)
    }

    "pass through when no rejection" in {
      val processor = RejectionProcessor.jsonRpc()
      val session = Session(requestBody = "test")
        .withResponse("success", ResponseSource.REMOTE)

      val result = Await.result(processor.process(session), 5.seconds)

      result.responseBody shouldBe Some("success")
      result.responseSource shouldBe ResponseSource.REMOTE
      result.getData[Int]("httpStatusCode") shouldBe None
    }
  }

  "RestApiRejectionProcessor" should {

    "convert rejection to REST API JSON format" in {
      val processor = RejectionProcessor.restApi()
      val rejection = Rejection(
        code = 404,
        message = "Not found",
        processorName = "Router"
      )
      val session = Session(requestBody = "test").copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      result.responseBody shouldBe defined
      val body = result.responseBody.get
      body should include(""""error":""")
      body should include(""""code": "404"""")
      body should include(""""message": "Not found"""")
      body should include(""""processor": "Router"""")
    }

    "map JSON-RPC codes to HTTP status codes" in {
      val processor = RejectionProcessor.restApi()

      // Parse error -> 400
      val session1 = Session(requestBody = "test")
        .copy(rejection = Some(Rejection(-32700, "Parse error", "Test")))
      val result1 = Await.result(processor.process(session1), 5.seconds)
      result1.getData[Int]("httpStatusCode") shouldBe Some(400)

      // Invalid request -> 400
      val session2 = Session(requestBody = "test")
        .copy(rejection = Some(Rejection(-32600, "Invalid request", "Test")))
      val result2 = Await.result(processor.process(session2), 5.seconds)
      result2.getData[Int]("httpStatusCode") shouldBe Some(400)

      // Method not found -> 404
      val session3 = Session(requestBody = "test")
        .copy(rejection = Some(Rejection(-32601, "Method not found", "Test")))
      val result3 = Await.result(processor.process(session3), 5.seconds)
      result3.getData[Int]("httpStatusCode") shouldBe Some(404)

      // Internal error -> 500
      val session4 = Session(requestBody = "test")
        .copy(rejection = Some(Rejection(-32603, "Internal error", "Test")))
      val result4 = Await.result(processor.process(session4), 5.seconds)
      result4.getData[Int]("httpStatusCode") shouldBe Some(500)
    }

    "use HTTP status codes directly when in 400-599 range" in {
      val processor = RejectionProcessor.restApi()

      val session = Session(requestBody = "test")
        .copy(rejection = Some(Rejection(503, "Service unavailable", "Test")))
      val result = Await.result(processor.process(session), 5.seconds)

      result.getData[Int]("httpStatusCode") shouldBe Some(503)
    }

    "use default HTTP status for unknown codes" in {
      val processor = RejectionProcessor.restApi(defaultHttpStatus = 500)

      val session = Session(requestBody = "test")
        .copy(rejection = Some(Rejection(9999, "Unknown error", "Test")))
      val result = Await.result(processor.process(session), 5.seconds)

      result.getData[Int]("httpStatusCode") shouldBe Some(500)
    }
  }

  "CustomRejectionProcessor" should {

    "use custom formatter" in {
      val formatter = (r: Rejection, s: Session) => s"""Custom error: ${r.message}"""
      val statusMapper = (r: Rejection, s: Session) => 418  // I'm a teapot
      val processor = RejectionProcessor.custom(formatter, statusMapper)

      val rejection = Rejection(999, "Test error", "Test")
      val session = Session(requestBody = "test").copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      result.responseBody shouldBe Some("Custom error: Test error")
      result.getData[Int]("httpStatusCode") shouldBe Some(418)
    }

    "access session data in formatter" in {
      val formatter = (r: Rejection, s: Session) => {
        val reqId = s.getData[String]("requestId").getOrElse("unknown")
        s"""{"error": "${r.message}", "requestId": "$reqId"}"""
      }
      val statusMapper = (r: Rejection, s: Session) => 500
      val processor = RejectionProcessor.custom(formatter, statusMapper)

      val rejection = Rejection(500, "Error", "Test")
      val session = Session(requestBody = "test")
        .putData("requestId", "req-123")
        .copy(rejection = Some(rejection))

      val result = Await.result(processor.process(session), 5.seconds)

      val body = result.responseBody.get
      body should include(""""requestId": "req-123"""")
    }
  }
}
