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
        new TelemetryStdoutStore(60000L, "simple")

      case "stdout" :: params =>
        params.mkString("://").split(":").toList match {
          case interval :: Nil =>
            new TelemetryStdoutStore(interval.toInt, "simple")
          case interval :: format :: _ =>
            new TelemetryStdoutStore(interval.toInt, format)
          case _ =>
            new TelemetryStdoutStore(60000L, "simple")
        }

      case "prometheus" :: _ =>
        new TelemetryStorePrometheus()

      case "log" :: level :: _ =>
        new TelemetryStoreLog(level)

      case "log" :: Nil =>
        new TelemetryStoreLog("info")

      case _ =>
        new TelemetryStdoutStore(60, "simple")
    }
  }
}
