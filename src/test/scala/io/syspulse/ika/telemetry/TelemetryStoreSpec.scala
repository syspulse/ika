package io.syspulse.ika.telemetry

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import io.syspulse.ika.processor.ai.AiTokens

class TelemetryStoreSpec extends AnyWordSpec with Matchers {

  "TelemetryStore.toCsv" should {
    "produce a single data line (no header)" in {
      val t = new Telemetry()
      t.inc("requests.total", 10)
      val csv = TelemetryStore.toCsv(t, withHeader = false)
      csv.linesIterator.toList should have size 1
    }

    "produce header + single data line when header enabled" in {
      val t = new Telemetry()
      t.inc("requests.total", 10)
      val csv = TelemetryStore.toCsv(t, withHeader = true)
      val lines = csv.linesIterator.toList
      lines should have size 2
      lines.head should startWith("ts,")
      lines.head should include("requests.total")
      val headers = lines.head.split(',')
      val values  = lines(1).split(',')
      values(headers.indexOf("requests.total")) shouldBe "10"
    }

    "produce only columnar rows (no flat scalar row) when columnar data is present" in {
      val t = new Telemetry()
      t.inc("requests.total", 5)
      val aiTokens = new AiTokens()
      t.registerData("ai.tokens", aiTokens)
      aiTokens.addTokens(1L, 2L, "acme", "openai", "gpt-4o", 10, 5)
      val csv = TelemetryStore.toCsv(t, withHeader = false)
      val lines = csv.linesIterator.toList
      lines should have size 1
      lines.head should include("acme")
      lines.head should not include "requests.total"
    }

    "include all registered counter values in one row" in {
      val t = new Telemetry()
      t.inc("alpha", 1)
      t.inc("beta",  2)
      val csv = TelemetryStore.toCsv(t, withHeader = true)
      val lines = csv.linesIterator.toList
      // Always 2 lines regardless of number of data entries
      lines should have size 2
      lines.head should include("alpha")
      lines.head should include("beta")
      val headers = lines.head.split(',')
      val values  = lines(1).split(',')
      values(headers.indexOf("alpha")) shouldBe "1"
      values(headers.indexOf("beta"))  shouldBe "2"
    }
  }

  "TelemetryStore.toJson" should {
    "produce a single flat JSON object" in {
      val t = new Telemetry()
      t.inc("x", 2)
      val json = TelemetryStore.toJson(t)
      // Single-line flat object
      json.linesIterator.toList should have size 1
      json should startWith("{")
      json should include("\"ts\":")
      json should include("\"x\":\"2\"")
    }
  }

  "Telemetry.toString" should {
    "emit timestamp and key=value metrics" in {
      val t = new Telemetry()
      t.inc("requests.total", 7)
      val s = t.toString
      s should startWith("[")
      s should include("requests.total=7")
    }
  }

  "TelemetryStore.formatOutput" should {
    "use toString when no format is set" in {
      val t = new Telemetry()
      t.inc("a", 1)
      val line = TelemetryStore.formatOutput(t, format = None, csvHeader = false)
      line should startWith("[")
      line should include("a=1")
    }

    "ignore format=tostring as serialization" in {
      TelemetryStore.parseSerializationFormat("toString") shouldBe None
      TelemetryStore.parseSerializationFormat("tostring") shouldBe None
    }
  }

  "TelemetryStore.parseUri" should {
    "leave format unset by default" in {
      val c = TelemetryStore.parseUri("stdout://60000")
      c.format shouldBe None
    }

    "parse stdout interval and format" in {
      val c = TelemetryStore.parseUri("stdout://10000?format=csv&header=false")
      c.sink shouldBe TelemetrySink.Stdout
      c.intervalMs shouldBe 10000L
      c.format shouldBe Some("csv")
      c.csvHeader shouldBe false
    }

    "parse stderr" in {
      val c = TelemetryStore.parseUri("stderr://5000:json")
      c.sink shouldBe TelemetrySink.Stderr
      c.intervalMs shouldBe 5000L
      c.format shouldBe Some("json")
    }

    "parse file with rotation" in {
      val c = TelemetryStore.parseUri(
        "file:///tmp/ika-{yyyy}-{MM}-{dd}.csv?interval=30000&format=csv&rotate=1024"
      )
      c.sink shouldBe a[TelemetrySink.File]
      c.intervalMs shouldBe 30000L
      c.format shouldBe Some("csv")
      c.rotateMaxBytes shouldBe Some(1024L)
    }

    "map legacy detailed to csv without header" in {
      val c = TelemetryStore.parseUri("stdout://60000:detailed")
      c.format shouldBe Some("csv")
      c.csvHeader shouldBe false
    }

    "parse publish=new for stdout" in {
      val c = TelemetryStore.parseUri("stdout://10000?format=csv&publish=new")
      c.publishPolicy shouldBe PublishPolicy.New
    }

    "parse publish=always for file" in {
      val c = TelemetryStore.parseUri("file:///tmp/t.csv?format=csv&publish=always")
      c.publishPolicy shouldBe PublishPolicy.Always
    }

    "default publish=always when not specified" in {
      val c = TelemetryStore.parseUri("stdout://10000?format=csv")
      c.publishPolicy shouldBe PublishPolicy.Always
    }
  }
}
