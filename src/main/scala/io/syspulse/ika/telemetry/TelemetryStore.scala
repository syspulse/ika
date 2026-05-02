package io.syspulse.ika.telemetry

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem

/**
 * TelemetryStore manages telemetry collection and publishing to external systems.
 *
 * The store owns a Telemetry instance and handles periodic publishing.
 *
 * Implementations:
 * - StdoutStore: Prints metrics to stdout periodically
 * - PrometheusStore: Exposes metrics for Prometheus scraping
 * - LogStore: Writes metrics to application logs
 */
trait TelemetryStore {
  /**
   * Get the managed telemetry instance
   */
  def telemetry: Telemetry

  /**
   * Publish metrics from telemetry
   */
  def publish(): Unit

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
 * StdoutStore prints telemetry metrics to stdout.
 *
 * Format: One line per metric with timestamp
 * Example: [2026-04-12T17:30:00] requests.total=1234 cache.hits=890
 */
class StdoutStore(
  interval: Long = 60000L,
  format: String = "simple"  // "simple" or "detailed"
)(implicit actorSystem: ActorSystem, ec: ExecutionContext) extends TelemetryStore {

  private val log = Logger(s"${this.getClass.getSimpleName}")
  private var scheduler: Option[akka.actor.Cancellable] = None

  // Create and own the telemetry instance
  val telemetry: Telemetry = Telemetry()

  private def formatBytes(bytes: Long): String = {
    val b = math.max(0L, bytes)
    if (b < 1024L) s"${b}B"
    else if (b < 1024L * 1024L) f"${b.toDouble / 1024.0}%.1fKiB"
    else if (b < 1024L * 1024L * 1024L) f"${b.toDouble / (1024.0 * 1024.0)}%.1fMiB"
    else if (b < 1024L * 1024L * 1024L * 1024L) f"${b.toDouble / (1024.0 * 1024.0 * 1024.0)}%.2fGiB"
    else f"${b.toDouble / (1024.0 * 1024.0 * 1024.0 * 1024.0)}%.2fTiB"
  }

  def publish(): Unit = {
    val timestamp = java.time.LocalDateTime.now().toString

    format match {
      case "detailed" =>
        // Print all telemetry data in a single line (key=value ...)
        val kv = telemetry.toFlatKV.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(",")
        println(s"[$timestamp] $kv")

      case "simple" | _ =>
        // Print one-line summary with key metrics
        val requests = telemetry.getCounter("requests.total")
        val responses = telemetry.getCounter("responses.total")
        val errors = telemetry.getCounter("errors.total")
        val cacheHits = telemetry.getCounter("cache.hits")
        val cacheMisses = telemetry.getCounter("cache.misses")
        val avgDuration = telemetry.getAverageTime("request.duration")
        val reqBytes = telemetry.getCounter("requests.bytes.total")
        val rspBytes = telemetry.getCounter("responses.bytes.total")

        val cacheHitRate = if (cacheHits + cacheMisses > 0) {
          (cacheHits.toDouble / (cacheHits + cacheMisses).toDouble * 100.0)
        } else 0.0

        println(f"[$timestamp] req=$requests%,d, rsp=$responses%,d, err=$errors%,d, bytes=[${formatBytes(reqBytes)},${formatBytes(rspBytes)}], cache_hit=$cacheHits%,d(${cacheHitRate}%.1f%%), lat=${avgDuration}%.2fms")
    }
  }

  override def start(): Unit = {
    if (interval > 0 && scheduler.isEmpty) {      
      scheduler = Some(
        actorSystem.scheduler.scheduleAtFixedRate(
          initialDelay = interval.millis,
          interval = interval.millis
        ) { () =>
          publish()
        }
      )
    }
  }

  override def stop(): Unit = {
    scheduler.foreach(_.cancel())
    scheduler = None
    log.info("Stopped StdoutStore")
  }
}

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
class PrometheusStore extends TelemetryStore {

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

/**
 * LogStore writes metrics to application logs
 */
class LogStore(
  level: String = "INFO"  // "DEBUG", "INFO", "WARN"
) extends TelemetryStore {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  // Create and own the telemetry instance
  val telemetry: Telemetry = Telemetry()

  def publish(): Unit = {
    val summary = telemetry.summary()

    level.toUpperCase match {
      case "DEBUG" => log.debug(s"\n$summary")
      case "WARN" => log.warn(s"\n$summary")
      case "INFO" | _ => log.info(s"\n$summary")
    }
  }
}

object TelemetryStore {
  /**
   * Create store from URI notation
   *
   * Examples:
   * - stdout://60000        - Stdout every 60 seconds
   * - stdout://10000:detailed - Stdout with detailed format
   * - prometheus://      - Prometheus format
   * - log://info         - Log at INFO level
   */
  def fromUri(uri: String)(implicit actorSystem: ActorSystem, ec: ExecutionContext): TelemetryStore = {
    uri.split("://").toList match {
      case "stdout" :: Nil =>
        new StdoutStore(60000L, "simple")

      case "stdout" :: params =>
        params.mkString("://").split(":").toList match {
          case interval :: Nil =>
            new StdoutStore(interval.toInt, "simple")
          case interval :: format :: _ =>
            new StdoutStore(interval.toInt, format)
          case _ =>
            new StdoutStore(60000L, "simple")
        }

      case "prometheus" :: _ =>
        new PrometheusStore()

      case "log" :: level :: _ =>
        new LogStore(level)

      case "log" :: Nil =>
        new LogStore("info")

      case _ =>
        new StdoutStore(60, "simple")
    }
  }
}
