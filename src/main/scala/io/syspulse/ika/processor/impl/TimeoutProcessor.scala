package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger

import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext
import io.syspulse.ika.processor.{RequestProcessor, Session, Processor}
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * TimeoutProcessor sets the timeout configuration on the session.
 *
 * This allows downstream processors (like HttpProcessor) to use the timeout value
 * when making HTTP requests.
 *
 * Can also set retry delay if provided.
 */
class TimeoutProcessor(
  timeoutMs: Long,
  retryDelayMs: Option[Long] = None
)(implicit ec: ExecutionContext) extends RequestProcessor {

  private val log = Logger(s"${name}")

  def name: String = "Timeout"

  override def toString: String = s"${name}(${timeoutMs},${retryDelayMs})"    

  def processRequest(session: Session): Future[Session] = {
    log.debug(s"Setting timeout: ${timeoutMs}ms" + retryDelayMs.map(d => s", retry delay: ${d}ms").getOrElse(""))

    val updated = session.putData("timeoutMs", timeoutMs)
    val withDelay = retryDelayMs match {
      case Some(delay) => updated.putData("retryDelayMs", delay)
      case None => updated
    }

    Future.successful(withDelay)
  }
}

object TimeoutProcessor extends ProcessorConfigurable {
  override val tpe: String = "timeout"

  def apply(timeoutMs: Long)(implicit ec: ExecutionContext): TimeoutProcessor = {
    new TimeoutProcessor(timeoutMs, None)
  }

  def apply(timeoutMs: Long, retryDelayMs: Long)(implicit ec: ExecutionContext): TimeoutProcessor = {
    new TimeoutProcessor(timeoutMs, Some(retryDelayMs))
  }

  def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val ms = if (cfg.hasPath("timeout")) cfg.getLong("timeout")
    else if (cfg.hasPath("timeoutMs")) cfg.getLong("timeoutMs")
    else 0L

    val retryDelayMs =
      if (cfg.hasPath("retryDelay")) Some(cfg.getLong("retryDelay"))
      else if (cfg.hasPath("retryDelayMs")) Some(cfg.getLong("retryDelayMs"))
      else None

    Seq(new TimeoutProcessor(ms, retryDelayMs))
  }
}
