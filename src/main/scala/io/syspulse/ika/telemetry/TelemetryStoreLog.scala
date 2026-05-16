package io.syspulse.ika.telemetry

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem

/**
 * LogStore writes metrics to application logs
 */
class TelemetryStoreLog(
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
