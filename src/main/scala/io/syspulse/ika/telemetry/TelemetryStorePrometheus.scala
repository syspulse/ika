package io.syspulse.ika.telemetry

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem

/**
 * PrometheusStore exposes metrics in Prometheus format.
 *
 * Metrics are exposed via HTTP endpoint (typically /metrics) that Prometheus
 * can scrape. The format follows Prometheus text exposition format.
 *
 * Example output:
 * # TYPE requests_total counter
 * requests_total 1234
 * # TYPE cache_hits counter
 * cache_hits 890
 */
class TelemetryStorePrometheus extends TelemetryStore {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  // Create and own the telemetry instance
  val telemetry: Telemetry = Telemetry()

  /**
   * Convert telemetry to Prometheus text format
   */
  def toPrometheusFormat(): String = {
    val sb = new StringBuilder()

    // Counters
    telemetry.getAllCounters.toSeq.sortBy(_._1).foreach { case (name, value) =>
      val metricName = name.replace('.', '_').replace('-', '_')
      sb.append(s"# TYPE $metricName counter\n")
      sb.append(s"$metricName $value\n")
    }

    // Gauges
    telemetry.getAllGauges.toSeq.sortBy(_._1).foreach { case (name, value) =>
      val metricName = name.replace('.', '_').replace('-', '_')
      sb.append(s"# TYPE $metricName gauge\n")
      sb.append(s"$metricName $value\n")
    }

    // Histograms (simplified as summary)
    telemetry.getAllHistograms.toSeq.sortBy(_._1).foreach { case (name, (sum, count, _)) =>
      val metricName = name.replace('.', '_').replace('-', '_')
      sb.append(s"# TYPE ${metricName}_seconds summary\n")
      sb.append(s"${metricName}_seconds_sum ${sum / 1000.0}\n")
      sb.append(s"${metricName}_seconds_count $count\n")
    }

    // Uptime
    sb.append(s"# TYPE uptime_seconds gauge\n")
    sb.append(s"uptime_seconds ${telemetry.getUptimeMs / 1000.0}\n")

    sb.toString()
  }

  def publish(): Unit = {
    // This is typically called by an HTTP endpoint handler
    // The actual publishing happens when Prometheus scrapes the /metrics endpoint
    log.debug("Prometheus metrics ready for scraping")
  }

  /**
   * Get metrics in Prometheus format for HTTP endpoint
   */
  def getMetrics(): String = {
    toPrometheusFormat()
  }
}
