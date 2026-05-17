package io.syspulse.ika.telemetry

import com.typesafe.scalalogging.Logger

/**
 * LogStore writes metrics to application logs (toString by default, or csv/json).
 */
class TelemetryStoreLog(
  level: String = "INFO",
  config: TelemetrySinkConfig = TelemetrySinkConfig(TelemetrySink.Stdout)
) extends TelemetryStore {

  private val log = Logger(getClass.getSimpleName)

  val telemetry: Telemetry = Telemetry()

  def publish(): Unit = {
    val line = TelemetryStore.formatOutput(telemetry, config.format, config.csvHeader)
    level.toUpperCase match {
      case "DEBUG" => log.debug(line)
      case "WARN"  => log.warn(line)
      case "ERROR" => log.error(line)
      case _       => log.info(line)
    }
  }
}
