package io.syspulse.ika.processor.impl

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import java.nio.file.Paths
import scala.io.Source

import io.syspulse.ika.processor.Session
import io.syspulse.ika.telemetry.Telemetry
import io.syspulse.ika.processor.ai.AIRouterProcessor
import io.syspulse.ika.processor.ai.AITokensProcessor
import akka.http.scaladsl.model.HttpHeader
import akka.util.ByteString

class AIProcessorsSpec extends AnyWordSpec with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.global
  private val providers = Map(
    "openai" -> "https://api.openai.com",
    "anthropic" -> "https://api.anthropic.com"
  )

  private val testDir = Paths.get(System.getProperty("user.dir"), "test", "ai")
  private def loadAiFixture(name: String): String =
    Source.fromFile(testDir.resolve(name).toFile, "UTF-8").mkString.trim

  "AIRouterProcessor" should {
    "extract model with provider prefix and set pool" in {
      val processor = AIRouterProcessor(providers)
      val request = """{"model": "openai/gpt-4o-mini", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("openai/gpt-4o-mini")
      result.getData[String]("provider") shouldBe Some("openai")
      result.getData[String]("pool") shouldBe Some("openai")
      result.getData[String]("destination") shouldBe Some("https://api.openai.com")
    }

    "route fixture request (suffix appended later by HttpProcessor)" in {
      val processor = new AIRouterProcessor(providerUris = providers)
      val request = loadAiFixture("REQ_chat_completion-1.json")
      val session = Session(requestBody = ByteString(request)).putData("http.uriSuffix", "/test/1")

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("openai/gpt-4o-mini")
      result.getData[String]("provider") shouldBe Some("openai")
      result.getData[String]("pool") shouldBe Some("openai")
      result.getData[String]("destination") shouldBe Some("https://api.openai.com")
      result.getData[Boolean]("http.destinationHasSuffix") shouldBe None
    }

    "inject provider API key header (fixture request)" in {
      val processor = new AIRouterProcessor(
        providerUris = providers,
        providerApiHeaderName = Map("openai" -> "Authorization"),
        providerApiHeaderValue = Map("openai" -> "Bearer sk-test")
      )

      val request = loadAiFixture("REQ_chat_completion-1.json")
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      val hs: Seq[HttpHeader] = result.requestHeaders
      hs.exists(h => h.lowercaseName() == "authorization" && h.value() == "Bearer sk-test") shouldBe true
    }

    "extract model without provider prefix and use default" in {
      val processor = AIRouterProcessor(providers)
      val request = """{"model": "gpt-4", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("gpt-4")
      result.getData[String]("provider") shouldBe Some("openai")  // default provider
      result.getData[String]("pool") shouldBe Some("openai")
      result.getData[String]("destination") shouldBe Some("https://api.openai.com")
    }

    "extract model with custom provider" in {
      val processor = AIRouterProcessor(providers + ("anthropic" -> "https://api.anthropic.com"))
      val request = """{"model": "anthropic/claude-3-opus", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("anthropic/claude-3-opus")
      result.getData[String]("provider") shouldBe Some("anthropic")
      result.getData[String]("pool") shouldBe Some("anthropic")
      result.getData[String]("destination") shouldBe Some("https://api.anthropic.com")
    }

    "use custom model-to-pool mapping" in {
      val processor = new AIRouterProcessor(
        providerUris = providers + ("claude" -> "https://api.claude.com"),
        modelProviderMapping = Map("gpt-4-custom" -> "claude")
      )
      val request = """{"model": "gpt-4-custom", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("gpt-4-custom")
      result.getData[String]("provider") shouldBe Some("claude")
      result.getData[String]("pool") shouldBe Some("claude")
      result.getData[String]("destination") shouldBe Some("https://api.claude.com")
    }

    "reject request without model field" in {
      val processor = AIRouterProcessor(providers)
      val request = """{"messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.code shouldBe -32602
      result.rejection.get.message should include("Missing required field: model")
    }

    "reject request with invalid JSON" in {
      val processor = AIRouterProcessor(providers)
      val request = """not valid json"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.code shouldBe -32700
      result.rejection.get.message should include("Parse error")
    }

    "reject request with non-string model field" in {
      val processor = AIRouterProcessor(providers)
      val request = """{"model": 123, "messages": []}"""
      val session = Session(requestBody = ByteString(request))

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
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))

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
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))
        .putData("telemetry", telemetry)

      Await.result(processor.process(session), 5.seconds)

      telemetry.getCounter("ai.tokens.total") shouldBe 50
    }

    "handle response without usage field gracefully" in {
      val processor = AITokensProcessor()
      val response = """{"id": "chatcmpl-123", "choices": []}"""
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("totalTokens") shouldBe None
    }

    "handle non-JSON response gracefully (streaming)" in {
      val processor = AITokensProcessor()
      val response = """data: {"delta": "text"}"""
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("totalTokens") shouldBe None
    }

    "handle missing response body gracefully" in {
      val processor = AITokensProcessor()
      val session = Session(requestBody = ByteString("test"), responseBody = None)

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
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("promptTokens") shouldBe None
      result.getData[Int]("completionTokens") shouldBe None
      result.getData[Int]("totalTokens") shouldBe Some(100)
    }
  }
}
