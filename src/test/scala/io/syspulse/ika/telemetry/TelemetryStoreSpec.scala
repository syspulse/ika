package io.syspulse.ika.telemetry

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TelemetryStoreSpec extends AnyWordSpec with Matchers {

  "TelemetryStore.toCsv" should {
    "include header row when requested" in {
      val t = new Telemetry()
      t.inc("requests.total", 10)
      val csv = TelemetryStore.toCsv(t, withHeader = true, timestamp = "2026-05-16T00:00:00Z")
      val lines = csv.linesIterator.toList
      lines should have size 2
      lines.head should startWith("timestamp,")
      lines.head should include("requests.total")
      val headers = lines.head.split(',')
      val values = lines(1).split(',')
      values(headers.indexOf("requests.total")) shouldBe "10"
    }

    "omit header when disabled" in {
      val t = new Telemetry()
      t.inc("a", 1)
      val csv = TelemetryStore.toCsv(t, withHeader = false, timestamp = "ts")
      csv.linesIterator.toList should have size 1
      csv should startWith("ts,1")
      csv.split(',').length shouldBe 3 // timestamp, a, uptime.ms
    }
  }

  "TelemetryStore.toJson" should {
    "emit timestamp and metrics object" in {
      val t = new Telemetry()
      t.inc("x", 2)
      val json = TelemetryStore.toJson(t, timestamp = "ts")
      json should include("\"timestamp\":\"ts\"")
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
  }
}
