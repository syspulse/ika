package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{RequestProcessor, Session}

/**
 * ThrottleProcessor implements global request throttling by sleeping before processing requests.
 *
 * This is useful for:
 * - Rate limiting to upstream services
 * - Preventing overload
 * - Respecting API quotas
 *
 * The throttling can be:
 * - Global: All requests across all threads are throttled together (default)
 * - Per-instance: Each processor instance throttles independently
 */
class ThrottleProcessor(
  throttleMs: Long,
  global: Boolean = true
)(implicit ec: ExecutionContext) extends RequestProcessor {

  private val log = Logger(s"${name}")

  def name: String = "Throttle"

  /**
   * Block/sleep for the throttle duration
   */
  private def block(): Unit = {
    if (throttleMs == 0L) return

    if (global) {
      // Global throttling - synchronized across all threads
      ThrottleProcessor.GlobalLock.synchronized {
        Thread.sleep(throttleMs)
      }
    } else {
      // Per-instance throttling
      this.synchronized {
        Thread.sleep(throttleMs)
      }
    }
  }

  def processRequest(session: Session): Future[Session] = {
    if (throttleMs > 0) {
      log.debug(s"Throttling request for ${throttleMs}ms")
      block()
    }
    Future.successful(session)
  }
}

object ThrottleProcessor {
  /**
   * Global lock object for cross-thread throttling
   */
  val GlobalLock = new Object()

  def apply(throttleMs: Long)(implicit ec: ExecutionContext): ThrottleProcessor = {
    new ThrottleProcessor(throttleMs, global = true)
  }

  def apply(throttleMs: Long, global: Boolean)(implicit ec: ExecutionContext): ThrottleProcessor = {
    new ThrottleProcessor(throttleMs, global)
  }
}
