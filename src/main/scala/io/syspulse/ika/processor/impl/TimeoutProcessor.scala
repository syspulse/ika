package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{RequestProcessor, Session}

/**
 * TimeoutProcessor sets the timeout configuration on the session.
 *
 * This allows downstream processors (like HttpClientProcessor) to use the timeout value
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

object TimeoutProcessor {
  def apply(timeoutMs: Long)(implicit ec: ExecutionContext): TimeoutProcessor = {
    new TimeoutProcessor(timeoutMs, None)
  }

  def apply(timeoutMs: Long, retryDelayMs: Long)(implicit ec: ExecutionContext): TimeoutProcessor = {
    new TimeoutProcessor(timeoutMs, Some(retryDelayMs))
  }
}
