package io.syspulse.ika.telemetry

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem


/**
 * StdoutStore prints telemetry metrics to stdout.
 *
 * Format: One line per metric with timestamp
 * Example: [2026-04-12T17:30:00] requests.total=1234 cache.hits=890
 */
class TelemetryStdoutStore(
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
