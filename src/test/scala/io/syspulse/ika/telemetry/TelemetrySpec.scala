package io.syspulse.ika.telemetry

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import io.syspulse.ika.processor.ai.AiTokens

class TelemetrySpec extends AnyWordSpec with Matchers {
  "Telemetry.toFlatKV" should {
    "not repeat provider name in ai.tokens keys when model is provider/model" in {
      val t        = new Telemetry()
      val aiTokens = new AiTokens()
      t.registerData("ai.tokens", aiTokens)
      aiTokens.addTokens(provider = "claude", model = "claude/claude-opus-4-0", inputTokens = 12, outputTokens = 0)

      val kv = t.toFlatKV

      kv.get("ai.tokens.claude.claude-opus-4-0.input") shouldBe Some("12")
      kv.contains("ai.tokens.claude.claude_claude-opus-4-0.input") shouldBe false
    }
  }

  "AiTokens" should {
    "accumulate tokens per provider/model" in {
      val a = new AiTokens()
      a.addTokens("openai", "gpt-4o", 100, 50)
      a.addTokens("openai", "gpt-4o", 10, 5)
      val kv = a.toFlatKV
      kv.get("ai.tokens.openai.gpt-4o.input")  shouldBe Some("110")
      kv.get("ai.tokens.openai.gpt-4o.output") shouldBe Some("55")
    }

    "reset resetOnFlush counters on flush" in {
      val a = new AiTokens()
      a.addTokens("openai", "gpt-4o", 100, 50)
      a.flush()
      val kv = a.toFlatKV
      kv.get("ai.tokens.openai.gpt-4o.input")  shouldBe Some("0")
      kv.get("ai.tokens.openai.gpt-4o.output") shouldBe Some("0")
    }

    "include all provider/model rows in toRecords" in {
      val a = new AiTokens()
      a.addTokens("claude", "claude-sonnet-4-6", 20, 10)
      a.addTokens("openai", "gpt-4o", 30, 15)
      val records = a.toRecords
      records should have size 2
      records.map(_("provider").toString).toSet shouldBe Set("claude", "openai")
    }

    "expose correct fields schema" in {
      val a = new AiTokens()
      val resetFields = a.fields.filter(_.resetOnFlush).map(_.name)
      resetFields should contain allOf ("input_tokens", "output_tokens", "errors")
      a.fields.map(_.name) should contain ("ts")
    }
  }
}

