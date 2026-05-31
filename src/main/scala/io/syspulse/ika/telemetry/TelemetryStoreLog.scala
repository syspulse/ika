package io.syspulse.ika.telemetry

import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem

/**
 * LogStore writes metrics to application logs (toString by default, or csv/json).
 * Publishes on the configured interval via the shared [[TelemetryStore]] scheduler.
 */
class TelemetryStoreLog(
  level: String = "INFO",
  config: TelemetrySinkConfig = TelemetrySinkConfig(TelemetrySink.Stdout)
)(implicit actorSystem: ActorSystem, ec: ExecutionContext) extends TelemetryStore {

  private val log = Logger(getClass.getSimpleName)

  val telemetry: Telemetry = Telemetry()

  override protected def intervalMs: Long = config.intervalMs

  def publish(): Unit = {
    if (config.publishPolicy == PublishPolicy.New && !telemetry.isDirty) return

    val line = TelemetryStore.formatOutput(telemetry, config.format, config.csvHeader, config.timestamp)
    level.toUpperCase match {
      case "DEBUG" => log.debug(line)
      case "WARN"  => log.warn(line)
      case "ERROR" => log.error(line)
      case _       => log.info(line)
    }
    telemetry.flush()
  }
}
