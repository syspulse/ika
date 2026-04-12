package io.syspulse.ika.telemetry

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem

/**
 * TelemetryPublisher publishes telemetry metrics to external systems.
 *
 * Implementations:
 * - StdoutPublisher: Prints metrics to stdout periodically
 * - PrometheusPublisher: Exposes metrics for Prometheus scraping
 * - Custom publishers can be implemented for other systems
 */
trait TelemetryPublisher {
  /**
   * Publish metrics from telemetry
   */
  def publish(telemetry: Telemetry): Unit

  /**
   * Start periodic publishing (if applicable)
   */
  def start(): Unit = {}

  /**
   * Stop publishing
   */
  def stop(): Unit = {}
}

/**
 * StdoutPublisher prints telemetry metrics to stdout.
 *
 * Format: One line per metric with timestamp
 * Example: [2026-04-12T17:30:00] requests.total=1234 cache.hits=890
 */
class StdoutPublisher(
  intervalSeconds: Int = 60,
  format: String = "simple"  // "simple" or "detailed"
)(implicit actorSystem: ActorSystem, ec: ExecutionContext) extends TelemetryPublisher {

  private val log = Logger(s"${this.getClass.getSimpleName}")
  private var scheduler: Option[akka.actor.Cancellable] = None

  def publish(telemetry: Telemetry): Unit = {
    val timestamp = java.time.LocalDateTime.now().toString

    format match {
      case "detailed" =>
        // Print detailed summary
        println(s"\n[$timestamp] Telemetry:")
        println(telemetry.summary())

      case "simple" | _ =>
        // Print one-line summary with key metrics
        val requests = telemetry.getCounter("requests.total")
        val responses = telemetry.getCounter("responses.total")
        val errors = telemetry.getCounter("errors.total")
        val cacheHits = telemetry.getCounter("cache.hits")
        val cacheMisses = telemetry.getCounter("cache.misses")
        val avgDuration = telemetry.getAverageTime("request.duration")

        val cacheHitRate = if (cacheHits + cacheMisses > 0) {
          (cacheHits.toDouble / (cacheHits + cacheMisses).toDouble * 100.0)
        } else 0.0

        println(f"[$timestamp] req=$requests%,d rsp=$responses%,d err=$errors%,d cache_hit=$cacheHits%,d(${cacheHitRate}%.1f%%) avg=${avgDuration}%.2fms uptime=${telemetry.getUptimeMs}%,dms")
    }
  }

  override def start(): Unit = {
    if (intervalSeconds > 0 && scheduler.isEmpty) {
      log.info(s"Starting StdoutPublisher with ${intervalSeconds}s interval")

      // Get the telemetry instance from the actor system (should be stored there)
      // For now, we'll receive it via publish() calls
      scheduler = Some(
        actorSystem.scheduler.scheduleAtFixedRate(
          initialDelay = intervalSeconds.seconds,
          interval = intervalSeconds.seconds
        ) { () =>
          // Note: This requires telemetry to be accessible
          // In practice, the telemetry should be passed or stored globally
          log.debug("Periodic telemetry publish (requires telemetry instance)")
        }
      )
    }
  }

  override def stop(): Unit = {
    scheduler.foreach(_.cancel())
    scheduler = None
    log.info("Stopped StdoutPublisher")
  }
}

/**
 * PrometheusPublisher exposes metrics in Prometheus format.
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
class PrometheusPublisher extends TelemetryPublisher {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  /**
   * Convert telemetry to Prometheus text format
   */
  def toPrometheusFormat(telemetry: Telemetry): String = {
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

  def publish(telemetry: Telemetry): Unit = {
    // This is typically called by an HTTP endpoint handler
    // The actual publishing happens when Prometheus scrapes the /metrics endpoint
    log.debug("Prometheus metrics ready for scraping")
  }

  /**
   * Get metrics in Prometheus format for HTTP endpoint
   */
  def getMetrics(telemetry: Telemetry): String = {
    toPrometheusFormat(telemetry)
  }
}

/**
 * LogPublisher writes metrics to application logs
 */
class LogPublisher(
  level: String = "INFO"  // "DEBUG", "INFO", "WARN"
) extends TelemetryPublisher {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  def publish(telemetry: Telemetry): Unit = {
    val summary = telemetry.summary()

    level.toUpperCase match {
      case "DEBUG" => log.debug(s"\n$summary")
      case "WARN" => log.warn(s"\n$summary")
      case "INFO" | _ => log.info(s"\n$summary")
    }
  }
}

object TelemetryPublisher {
  /**
   * Create publisher from URI notation
   *
   * Examples:
   * - stdout://60        - Stdout every 60 seconds
   * - stdout://60:detailed - Stdout with detailed format
   * - prometheus://      - Prometheus format
   * - log://info         - Log at INFO level
   */
  def fromUri(uri: String)(implicit actorSystem: ActorSystem, ec: ExecutionContext): TelemetryPublisher = {
    uri.split("://").toList match {
      case "stdout" :: Nil =>
        new StdoutPublisher(60, "simple")

      case "stdout" :: params =>
        params.mkString("://").split(":").toList match {
          case interval :: Nil =>
            new StdoutPublisher(interval.toInt, "simple")
          case interval :: format :: _ =>
            new StdoutPublisher(interval.toInt, format)
          case _ =>
            new StdoutPublisher(60, "simple")
        }

      case "prometheus" :: _ =>
        new PrometheusPublisher()

      case "log" :: level :: _ =>
        new LogPublisher(level)

      case "log" :: Nil =>
        new LogPublisher("info")

      case _ =>
        new StdoutPublisher(60, "simple")
    }
  }
}
