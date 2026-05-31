package io.syspulse.ika.telemetry

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import io.syspulse.ika.processor.ai.TelemetryDataAiTokens

class TelemetrySpec extends AnyWordSpec with Matchers {
  "Telemetry.toFlatKV" should {
    "not repeat provider name in ai.tokens keys when model is provider/model" in {
      val t        = new Telemetry()
      val aiTokens = new TelemetryDataAiTokens()
      t.registerData("ai.tokens", aiTokens)
      aiTokens.addTokens(tid = 0L, pid = 0L, customerId = "", provider = "claude", model = "claude/claude-opus-4-0", inputTokens = 12, outputTokens = 0)

      val kv = t.toFlatKV

      kv.get("ai.tokens.0.0._.claude.claude-opus-4-0.input") shouldBe Some("12")
      kv.contains("ai.tokens.0.0._.claude.claude_claude-opus-4-0.input") shouldBe false
    }
  }

  "TelemetryDataAiTokens" should {
    "accumulate tokens per provider/model" in {
      val a = new TelemetryDataAiTokens()
      a.addTokens(0L, 0L, "", "openai", "gpt-4o", 100, 50)
      a.addTokens(0L, 0L, "", "openai", "gpt-4o", 10, 5)
      val kv = a.toFlatKV
      kv.get("ai.tokens.0.0._.openai.gpt-4o.input")  shouldBe Some("110")
      kv.get("ai.tokens.0.0._.openai.gpt-4o.output") shouldBe Some("55")
    }

    "track lifetime total separately from flushed input/output" in {
      val a = new TelemetryDataAiTokens()
      a.addTokens(0L, 0L, "", "openai", "gpt-4o", 100, 50)
      a.flush()
      val kv = a.toFlatKV
      kv.get("ai.tokens.0.0._.openai.gpt-4o.input")  shouldBe Some("0")
      kv.get("ai.tokens.0.0._.openai.gpt-4o.output") shouldBe Some("0")
      kv.get("ai.tokens.0.0._.openai.gpt-4o.total")  shouldBe Some("150")
    }

    "reset resetOnFlush counters on flush" in {
      val a = new TelemetryDataAiTokens()
      a.addTokens(0L, 0L, "", "openai", "gpt-4o", 100, 50)
      a.flush()
      val kv = a.toFlatKV
      kv.get("ai.tokens.0.0._.openai.gpt-4o.input")  shouldBe Some("0")
      kv.get("ai.tokens.0.0._.openai.gpt-4o.output") shouldBe Some("0")
    }

    "include all provider/model rows in toRecords" in {
      val a = new TelemetryDataAiTokens()
      a.addTokens(0L, 0L, "", "claude", "claude-sonnet-4-6", 20, 10)
      a.addTokens(0L, 0L, "", "openai", "gpt-4o", 30, 15)
      val records = a.toRecords
      records should have size 2
      records.map(_("provider").toString).toSet shouldBe Set("claude", "openai")
    }

    "include tid, pid, customer_id in toRecords" in {
      val a = new TelemetryDataAiTokens()
      a.addTokens(42L, 7L, "acme", "openai", "gpt-4o", 10, 5)
      val records = a.toRecords
      records should have size 1
      val rec = records.head
      rec("tid")         shouldBe 42L
      rec("pid")         shouldBe 7L
      rec("customer_id") shouldBe "acme"
      rec("provider")    shouldBe "openai"
      rec("model")       shouldBe "gpt-4o"
      rec("input_tokens")  shouldBe 10L
      rec("output_tokens") shouldBe 5L
    }

    "exclude flushed zero-value entries from toRecords" in {
      val a = new TelemetryDataAiTokens()
      a.addTokens(400L, 13L, "customer-1", "openai", "gpt-4o-mini", 10, 5)
      a.flush()
      // Now add data for claude only
      a.addTokens(400L, 13L, "customer-1", "claude", "claude-opus-4-0", 12, 51)
      val records = a.toRecords
      records should have size 1
      records.head("provider") shouldBe "claude"
      records.head("input_tokens") shouldBe 12L
    }

    "be dirty after addTokens and clean after flush" in {
      val a = new TelemetryDataAiTokens()
      a.isDirty shouldBe false
      a.addTokens(0L, 0L, "", "openai", "gpt-4o", 5, 3)
      a.isDirty shouldBe true
      a.flush()
      a.isDirty shouldBe false
    }

    "expose correct fields schema" in {
      val a = new TelemetryDataAiTokens()
      val resetFields = a.fields.filter(_.resetOnFlush).map(_.name)
      resetFields should contain allOf ("input_tokens", "output_tokens", "errors")
      a.fields.map(_.name) should contain allOf ("ts", "tid", "pid", "customer_id")
    }
  }
}
