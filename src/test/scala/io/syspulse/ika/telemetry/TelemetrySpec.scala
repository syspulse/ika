package io.syspulse.ika.telemetry

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TelemetrySpec extends AnyWordSpec with Matchers {
  "Telemetry.toFlatKV" should {
    "not repeat provider name in ai.tokens keys when model is provider/model" in {
      val t = new Telemetry()
      t.addAiTokens(provider = "claude", model = "claude/claude-opus-4-0", inputTokens = 12, outputTokens = 0)

      val kv = t.toFlatKV

      kv.get("ai.tokens.claude.claude-opus-4-0.input") shouldBe Some("12")
      kv.contains("ai.tokens.claude.claude_claude-opus-4-0.input") shouldBe false
    }
  }
}

