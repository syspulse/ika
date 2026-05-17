package io.syspulse.ika.processor.core

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import java.nio.file.Paths
import scala.io.Source
import com.typesafe.config.ConfigFactory

import io.syspulse.ika.processor.Session
import io.syspulse.ika.telemetry.{Telemetry, TelemetryDataId}
import io.syspulse.ika.processor.ai.{AIRouterProcessor, AITokensProcessor, AiTokens}
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpHeader
import akka.util.ByteString
import spray.json._
import io.syspulse.ika.processor.core.MetaProcessor

class AIProcessorsSpec extends AnyWordSpec with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("ai-processors-spec")
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
      result.getData[String]("modelUpstream") shouldBe Some("gpt-4o-mini")
      result.getData[String]("provider") shouldBe Some("openai")
      result.getData[String]("pool") shouldBe Some("openai")
      result.getData[String]("destination") shouldBe Some("https://api.openai.com")
      result.requestBody.utf8String.parseJson.asJsObject.fields("model") shouldBe JsString("gpt-4o-mini")
    }

    "route fixture request (suffix appended later by HttpProcessor)" in {
      val processor = new AIRouterProcessor(providerUris = providers)
      val request = loadAiFixture("REQ_chat_completion-1.json")
      val session = Session(requestBody = ByteString(request)).putData("http.uriSuffix", "/test/1")

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("openai/gpt-4o-mini")
      result.getData[String]("modelUpstream") shouldBe Some("gpt-4o-mini")
      result.getData[String]("provider") shouldBe Some("openai")
      result.getData[String]("pool") shouldBe Some("openai")
      result.getData[String]("destination") shouldBe Some("https://api.openai.com")
      result.getData[Boolean]("http.destinationHasSuffix") shouldBe None
      result.requestBody.utf8String.parseJson.asJsObject.fields("model") shouldBe JsString("gpt-4o-mini")
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

    "inject provider custom headers" in {
      val processor = new AIRouterProcessor(
        providerUris = providers,
        providerHeaders = Map("anthropic" -> Map("anthropic-version" -> "2023-06-01"))
      )

      val request = """{"model": "anthropic/claude-3-haiku", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      val hs: Seq[HttpHeader] = result.requestHeaders
      hs.exists(h => h.lowercaseName() == "anthropic-version" && h.value() == "2023-06-01") shouldBe true
    }

    "parse provider custom headers from config" in {
      val cfg = ConfigFactory.parseString("""
        type = "ai_router://"
        providers {
          claude {
            uri = "https://api.anthropic.com"
            api_key = "sk-ant-test"
            api_key_header_name = "x-api-key"
            api_key_header_value = "%s"
            headers {
              "anthropic-version" = "2023-06-01"
            }
          }
        }
      """)
      val processor = AIRouterProcessor.fromConfig("ai_router_test", cfg).head
      val request = """{"model": "claude/claude-3-haiku", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      val hs: Seq[HttpHeader] = result.requestHeaders
      hs.exists(h => h.lowercaseName() == "x-api-key" && h.value() == "sk-ant-test") shouldBe true
      hs.exists(h => h.lowercaseName() == "anthropic-version" && h.value() == "2023-06-01") shouldBe true
    }

    "extract model without provider prefix and use default" in {
      val processor = AIRouterProcessor(providers)
      val request = """{"model": "gpt-4", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("gpt-4")
      result.getData[String]("modelUpstream") shouldBe Some("gpt-4")
      result.getData[String]("provider") shouldBe Some("openai")  // default provider
      result.getData[String]("pool") shouldBe Some("openai")
      result.getData[String]("destination") shouldBe Some("https://api.openai.com")
      result.requestBody.utf8String.parseJson.asJsObject.fields("model") shouldBe JsString("gpt-4")
    }

    "extract model with custom provider" in {
      val processor = AIRouterProcessor(providers + ("anthropic" -> "https://api.anthropic.com"))
      val request = """{"model": "anthropic/claude-3-opus", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model") shouldBe Some("anthropic/claude-3-opus")
      result.getData[String]("modelUpstream") shouldBe Some("claude-3-opus")
      result.getData[String]("provider") shouldBe Some("anthropic")
      result.getData[String]("pool") shouldBe Some("anthropic")
      result.getData[String]("destination") shouldBe Some("https://api.anthropic.com")
      result.requestBody.utf8String.parseJson.asJsObject.fields("model") shouldBe JsString("claude-3-opus")
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
      result.getData[String]("modelUpstream") shouldBe Some("gpt-4-custom")
      result.getData[String]("provider") shouldBe Some("claude")
      result.getData[String]("pool") shouldBe Some("claude")
      result.getData[String]("destination") shouldBe Some("https://api.claude.com")
      result.requestBody.utf8String.parseJson.asJsObject.fields("model") shouldBe JsString("gpt-4-custom")
    }

    // ── prefix-based routing (no "provider/" slash) ──────────────────────────

    "resolve 'claude-haiku-4-5-20251001' to provider 'claude' via built-in prefix" in {
      val processor = new AIRouterProcessor(
        providerUris = providers + ("claude" -> "https://api.anthropic.com")
      )
      val request = """{"model": "claude-haiku-4-5-20251001", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("model")         shouldBe Some("claude-haiku-4-5-20251001")
      result.getData[String]("modelUpstream") shouldBe Some("claude-haiku-4-5-20251001")
      result.getData[String]("provider")      shouldBe Some("claude")
      result.getData[String]("pool")          shouldBe Some("claude")
      result.getData[String]("destination")   shouldBe Some("https://api.anthropic.com")
      result.requestBody.utf8String.parseJson.asJsObject.fields("model") shouldBe JsString("claude-haiku-4-5-20251001")
    }

    "resolve 'gpt-4o-mini' to provider 'openai' via built-in prefix" in {
      val processor = new AIRouterProcessor(providerUris = providers)
      val request = """{"model": "gpt-4o-mini", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("provider") shouldBe Some("openai")
      result.getData[String]("pool")     shouldBe Some("openai")
    }

    "resolve 'gemini-pro' to provider 'gemini' via built-in prefix" in {
      val processor = new AIRouterProcessor(
        providerUris = providers + ("gemini" -> "https://generativelanguage.googleapis.com")
      )
      val request = """{"model": "gemini-pro", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("provider") shouldBe Some("gemini")
      result.getData[String]("pool")     shouldBe Some("gemini")
    }

    "resolve 'grok-2' to provider 'grok' via built-in prefix" in {
      val processor = new AIRouterProcessor(
        providerUris = providers + ("grok" -> "https://api.x.ai")
      )
      val request = """{"model": "grok-2", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("provider") shouldBe Some("grok")
      result.getData[String]("pool")     shouldBe Some("grok")
    }

    "custom modelPrefixMapping overrides built-in prefix" in {
      val processor = new AIRouterProcessor(
        providerUris = providers + ("myrouter" -> "https://my.router"),
        // "claude" would normally match built-in "claude" prefix; override it
        modelPrefixMapping = Seq("claude" -> "myrouter")
      )
      val request = """{"model": "claude-3-opus", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("provider") shouldBe Some("myrouter")
    }

    "load custom modelPrefixMapping from config and apply it" in {
      val cfg = ConfigFactory.parseString("""
        type = "ai_router://"
        providers {
          meta {
            uri = "https://llama.meta.com"
          }
        }
        model_prefix {
          "llama" = "meta"
        }
      """)
      val processor = AIRouterProcessor.fromConfig("ai_router_prefix_test", cfg).head
      val request = """{"model": "llama-3-70b", "messages": []}"""
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[String]("provider") shouldBe Some("meta")
      result.getData[String]("pool")     shouldBe Some("meta")
    }

    // ─────────────────────────────────────────────────────────────────────────

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
    "extract and strip metadata from request payload" in {
      val processor = new AITokensProcessor(metadataUsageAttr = Some("{tid}-{pid}-{customer_id}"))
      val request =
        """{
          |  "metadata": { "pid": 7, "tid": 9, "customer_id": "c-1" },
          |  "model": "openai/gpt-4o-mini",
          |  "messages": []
          |}""".stripMargin
      val session = Session(requestBody = ByteString(request))

      val result = Await.result(processor.processRequest(session), 5.seconds)

      result.getData[Int]("pid") shouldBe Some(7)
      result.getData[Int]("tid") shouldBe Some(9)
      result.getData[String]("customer_id") shouldBe Some("c-1")

      val bodyObj = result.requestBody.utf8String.parseJson.asJsObject
      bodyObj.fields.contains("metadata") shouldBe false
    }

    "record token usage with customer_id from metadata to AiTokens" in {
      val telemetry = Telemetry()
      val tokens = new AITokensProcessor(metadataUsageAttr = Some("{tid}-{pid}-{customer_id}"))
      val router = AIRouterProcessor(providers)

      val request =
        """{
          |  "metadata": { "pid": 7, "tid": 9, "customer_id": "c-1" },
          |  "model": "openai/gpt-4o-mini",
          |  "messages": []
          |}""".stripMargin

      val s0 = Session(requestBody = ByteString(request)).putData("telemetry", telemetry)
      val s1 = Await.result(tokens.processRequest(s0), 5.seconds)
      val s2 = Await.result(router.processRequest(s1), 5.seconds)

      val response = """{
        "usage": {
          "prompt_tokens": 11,
          "completion_tokens": 22,
          "total_tokens": 33
        }
      }"""

      Await.result(tokens.processResponse(s2.copy(responseBody = Some(ByteString(response)))), 5.seconds)

      val ids = telemetry.getOrRegisterData("ai.ids", new TelemetryDataId())
      ids.getLong("tid") shouldBe Some(9L)
      ids.getLong("pid") shouldBe Some(7L)

      val records = telemetry.getOrRegisterData("ai.tokens", new AiTokens()).toRecords
      records should have size 1
      val rec = records.head
      rec("tid")           shouldBe 9L
      rec("pid")           shouldBe 7L
      rec("customer_id")   shouldBe "c-1"
      rec("provider")      shouldBe "openai"
      rec("input_tokens")  shouldBe 11L
      rec("output_tokens") shouldBe 22L
    }

    "extract metadata fields from template and record to AiTokens" in {
      val telemetry = Telemetry()
      val tokens = new AITokensProcessor(metadataUsageAttr = Some("org-{tenant}-user-{user_id}"))
      val meta = new MetaProcessor(Seq("x-meta"))

      val request =
        """{
          |  "metadata": { "tenant": "acme", "user_id": 42, "ignored": "x" },
          |  "model": "openai/gpt-4o-mini",
          |  "messages": []
          |}""".stripMargin

      val s1 = Await.result(
        meta.processRequest(Session(requestBody = ByteString(request)).putData("telemetry", telemetry)
          .addRequestHeader(akka.http.scaladsl.model.headers.RawHeader("x-meta", "tenant=acme,user_id=42"))),
        5.seconds
      )
      val s2 = Await.result(tokens.processRequest(s1), 5.seconds)
        .putData("provider", "openai").putData("model", "openai/gpt-4o-mini")

      s2.getData[String]("tenant") shouldBe Some("acme")
      s2.getData[Int]("user_id") shouldBe Some(42)
      s2.getData[String]("ignored") shouldBe None

      Await.result(tokens.processResponse(s2.copy(responseBody = Some(ByteString("""{ "usage": { "input_tokens": 4, "output_tokens": 6 } }""")))), 5.seconds)

      val records = telemetry.getOrRegisterData("ai.tokens", new AiTokens()).toRecords
      records should have size 1
      val rec = records.head
      rec("customer_id")   shouldBe ""  // no customer_id in this metadata
      rec("provider")      shouldBe "openai"
      rec("input_tokens")  shouldBe 4L
      rec("output_tokens") shouldBe 6L
    }

    "accumulate AiTokens across multiple requests with same customer_id" in {
      val telemetry = Telemetry()
      val tokens = new AITokensProcessor(metadataUsageAttr = Some("{tid}-{pid}-{customer_id}"))
      val router = AIRouterProcessor(providers)

      val request =
        """{
          |  "metadata": { "pid": 13, "tid": 400, "customer_id": "customer-1" },
          |  "model": "claude/claude-haiku-4-5-20251001",
          |  "messages": []
          |}""".stripMargin

      val base = Session(requestBody = ByteString(request)).putData("telemetry", telemetry)
      val s1 = Await.result(tokens.processRequest(base), 5.seconds)
      val s2 = Await.result(router.processRequest(s1), 5.seconds)

      val rsp1 = """{ "usage": { "input_tokens": 12, "output_tokens": 45 } }"""
      val rsp2 = """{ "usage": { "input_tokens": 3, "output_tokens": 5 } }"""

      Await.result(tokens.processResponse(s2.copy(responseBody = Some(ByteString(rsp1)))), 5.seconds)
      Await.result(tokens.processResponse(s2.copy(responseBody = Some(ByteString(rsp2)))), 5.seconds)

      val ids = telemetry.getOrRegisterData("ai.ids", new TelemetryDataId())
      ids.getLong("tid") shouldBe Some(400L)
      ids.getLong("pid") shouldBe Some(13L)

      val records = telemetry.getOrRegisterData("ai.tokens", new AiTokens()).toRecords
      records should have size 1
      val rec = records.head
      rec("tid")           shouldBe 400L
      rec("pid")           shouldBe 13L
      rec("customer_id")   shouldBe "customer-1"
      rec("provider")      shouldBe "claude"
      rec("input_tokens")  shouldBe 15L
      rec("output_tokens") shouldBe 50L
    }

    "record separate AiTokens rows per provider/model for same customer_id" in {
      val telemetry = Telemetry()
      val tokens = new AITokensProcessor(metadataUsageAttr = Some("{tid}-{pid}-{customer_id}"))

      val request =
        """{
          |  "metadata": { "pid": 13, "tid": 400, "customer_id": "customer-1" },
          |  "model": "claude/claude-haiku-4-5-20251001",
          |  "messages": []
          |}""".stripMargin

      // First: claude
      val s0 = Session(requestBody = ByteString(request)).putData("telemetry", telemetry)
      val s1 = Await.result(tokens.processRequest(s0), 5.seconds)
        .putData("provider", "claude")
        .putData("model", "claude/claude-haiku-4-5-20251001")

      Await.result(tokens.processResponse(s1.copy(responseBody = Some(ByteString("""{ "usage": { "input_tokens": 12, "output_tokens": 51 } }""")))), 5.seconds)

      // Second: openai, same customer_id
      val s2 = s1
        .putData("provider", "openai")
        .putData("model", "openai/gpt-4o-mini")

      Await.result(tokens.processResponse(s2.copy(responseBody = Some(ByteString("""{ "usage": { "input_tokens": 36, "output_tokens": 115 } }""")))), 5.seconds)

      val ids = telemetry.getOrRegisterData("ai.ids", new TelemetryDataId())
      ids.getLong("tid") shouldBe Some(400L)
      ids.getLong("pid") shouldBe Some(13L)

      val records = telemetry.getOrRegisterData("ai.tokens", new AiTokens()).toRecords
      records should have size 2
      records.forall(_("customer_id").toString == "customer-1") shouldBe true
      records.forall(_("tid") == 400L) shouldBe true
      records.forall(_("pid") == 13L) shouldBe true
      val byProvider = records.groupBy(_("provider").toString)
      byProvider("claude").head("input_tokens")  shouldBe 12L
      byProvider("claude").head("output_tokens") shouldBe 51L
      byProvider("openai").head("input_tokens")  shouldBe 36L
      byProvider("openai").head("output_tokens") shouldBe 115L
    }

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

    "extract token usage from Anthropic/Claude response format" in {
      val processor = AITokensProcessor()
      val response = """{
        "type": "message",
        "usage": {
          "input_tokens": 12,
          "output_tokens": 49
        }
      }"""
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("promptTokens") shouldBe Some(12)
      result.getData[Int]("completionTokens") shouldBe Some(49)
      result.getData[Int]("totalTokens") shouldBe Some(61)
    }

    "extract token usage from OpenAI Responses format" in {
      val processor = AITokensProcessor()
      val response = """{
        "object": "response",
        "usage": {
          "input_tokens": 12,
          "output_tokens": 19,
          "total_tokens": 31
        }
      }"""
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))

      val result = Await.result(processor.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.getData[Int]("promptTokens") shouldBe Some(12)
      result.getData[Int]("completionTokens") shouldBe Some(19)
      result.getData[Int]("totalTokens") shouldBe Some(31)
    }

    "add token usage to telemetry" in {
      val telemetry = Telemetry()
      val processor = AITokensProcessor()
      val response = """{
        "usage": {
          "input_tokens": 50,
          "output_tokens": 0,
          "total_tokens": 50
        }
      }"""
      val session = Session(requestBody = ByteString("test"), responseBody = Some(ByteString(response)))
        .putData("telemetry", telemetry)
        .putData("provider", "openai")
        .putData("model", "openai/gpt-4o-mini")

      Await.result(processor.process(session), 5.seconds)

      val kv = telemetry.getOrRegisterData("ai.tokens", new AiTokens()).toFlatKV
      (kv.getOrElse("ai.tokens.0.0._.openai.gpt-4o-mini.input", "0").toLong +
       kv.getOrElse("ai.tokens.0.0._.openai.gpt-4o-mini.output", "0").toLong) shouldBe 50L
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
