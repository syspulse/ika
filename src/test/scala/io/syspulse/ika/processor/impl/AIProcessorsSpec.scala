package io.syspulse.ika.processor.impl

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._

import io.syspulse.ika.processor.Session
import io.syspulse.ika.telemetry.Telemetry

class AIProcessorsSpec extends AnyWordSpec with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.global

  "AIRouterProcessor" should {
    "extract model with provider prefix and set pool" in {
      val processor = AIRouterProcessor()
      val request = """{"model": "openai/gpt-4o-mini", "messages": []}"""
      val session = Session(requestBody = request)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("openai/gpt-4o-mini")
      result.getData[String]("provider") shouldBe Some("openai")
      result.getData[String]("pool") shouldBe Some("openai")
    }

    "extract model without provider prefix and use default" in {
      val processor = AIRouterProcessor()
      val request = """{"model": "gpt-4", "messages": []}"""
      val session = Session(requestBody = request)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("gpt-4")
      result.getData[String]("provider") shouldBe Some("openai")  // default provider
      result.getData[String]("pool") shouldBe Some("openai")
    }

    "extract model with custom provider" in {
      val processor = AIRouterProcessor()
      val request = """{"model": "anthropic/claude-3-opus", "messages": []}"""
      val session = Session(requestBody = request)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("anthropic/claude-3-opus")
      result.getData[String]("provider") shouldBe Some("anthropic")
      result.getData[String]("pool") shouldBe Some("anthropic")
    }

    "use custom model-to-pool mapping" in {
      val mapping = Map("gpt-4-custom" -> "premium-pool")
      val processor = AIRouterProcessor(mapping)
      val request = """{"model": "gpt-4-custom", "messages": []}"""
      val session = Session(requestBody = request)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("gpt-4-custom")
      result.getData[String]("pool") shouldBe Some("premium-pool")
    }

    "reject request without model field" in {
      val processor = AIRouterProcessor()
      val request = """{"messages": []}"""
      val session = Session(requestBody = request)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.code shouldBe -32602
      result.rejection.get.message should include("Missing required field: model")
    }

    "reject request with invalid JSON" in {
      val processor = AIRouterProcessor()
      val request = """not valid json"""
      val session = Session(requestBody = request)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.code shouldBe -32700
      result.rejection.get.message should include("Parse error")
    }

    "reject request with non-string model field" in {
      val processor = AIRouterProcessor()
      val request = """{"model": 123, "messages": []}"""
      val session = Session(requestBody = request)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.code shouldBe -32602
      result.rejection.get.message should include("Invalid model field type")
    }
  }

  "AITokensProcessor" should {
    "extract token usage from response" in {
      val processor = AITokensProcessor()
      val response = """{
        "id": "chatcmpl-123",
        "choices": [{"message": {"content": "Hello"}}],
        "usage": {
          "prompt_tokens": 10,
          "completion_tokens": 20,
          "total_tokens": 30
        }
      }"""
      val session = Session(requestBody = "test", responseBody = Some(response))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("promptTokens") shouldBe Some(10)
      result.getData[Int]("completionTokens") shouldBe Some(20)
      result.getData[Int]("totalTokens") shouldBe Some(30)
    }

    "add token usage to telemetry" in {
      val telemetry = Telemetry()
      val processor = AITokensProcessor()
      val response = """{
        "usage": {
          "total_tokens": 50
        }
      }"""
      val session = Session(requestBody = "test", responseBody = Some(response))
        .putData("telemetry", telemetry)

      Await.result(processor.process(session), 5.seconds)

      telemetry.getCounter("ai.tokens.total") shouldBe 50
    }

    "handle response without usage field gracefully" in {
      val processor = AITokensProcessor()
      val response = """{"id": "chatcmpl-123", "choices": []}"""
      val session = Session(requestBody = "test", responseBody = Some(response))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("totalTokens") shouldBe None
    }

    "handle non-JSON response gracefully (streaming)" in {
      val processor = AITokensProcessor()
      val response = """data: {"delta": "text"}"""
      val session = Session(requestBody = "test", responseBody = Some(response))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("totalTokens") shouldBe None
    }

    "handle missing response body gracefully" in {
      val processor = AITokensProcessor()
      val session = Session(requestBody = "test", responseBody = None)

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("totalTokens") shouldBe None
    }

    "handle partial usage fields" in {
      val processor = AITokensProcessor()
      val response = """{
        "usage": {
          "total_tokens": 100
        }
      }"""
      val session = Session(requestBody = "test", responseBody = Some(response))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("promptTokens") shouldBe None
      result.getData[Int]("completionTokens") shouldBe None
      result.getData[Int]("totalTokens") shouldBe Some(100)
    }
  }
}
