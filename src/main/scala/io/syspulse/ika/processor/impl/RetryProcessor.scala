package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.Logger
import akka.actor.Scheduler

import io.syspulse.ika.processor.{WrapperProcessor, Processor, Session}

/**
 * RetryProcessor wraps a processor (or pipeline) and retries on failure.
 *
 * The retry logic:
 * - Executes the wrapped processor
 * - If it fails (Future.failed or rejection), wait and retry
 * - Uses the delay from session.retryDelayMs
 * - Increments session.retry counter
 * - Stops when session.retry >= session.maxRetry
 *
 * This processor uses akka.pattern.after for async delays between retries.
 */
class RetryProcessor(
  val wrapped: Processor,
  maxRetries: Int,
  delayMs: Long
)(implicit ec: ExecutionContext, scheduler: Scheduler) extends WrapperProcessor {

  private val log = Logger(s"${name}")

  def name: String = "Retry"

  /**
   * Execute processor with retry logic
   */
  def process(session: Session): Future[Session] = {
    if (session.isRejected) {
      return Future.successful(session)
    }

    // Initialize retry data if not already set
    val sessionWithRetry = session
      .putData("maxRetry", session.getData[Int]("maxRetry").getOrElse(maxRetries))
      .putData("retry", session.getData[Int]("retry").getOrElse(0))
      .putData("retryDelayMs", session.getData[Long]("retryDelayMs").getOrElse(delayMs))

    attemptWithRetry(sessionWithRetry)
  }

  /**
   * Attempt to execute the wrapped processor with retry logic
   */
  private def attemptWithRetry(session: Session): Future[Session] = {
    val retry = session.getData[Int]("retry").getOrElse(0)
    val maxRetry = session.getData[Int]("maxRetry").getOrElse(maxRetries)
    val retryDelay = session.getData[Long]("retryDelayMs").getOrElse(delayMs)

    log.debug(s"Attempting request (retry ${retry}/${maxRetry})")

    wrapped.process(session).recoverWith {
      case ex: Exception =>
        if (retry < maxRetry) {
          val nextSession = session
            .putData("retry", retry + 1)
            .putData("errorReason", s"${ex.getClass.getSimpleName}: ${ex.getMessage}")

          log.warn(s"Request failed (retry ${retry}/${maxRetry}): ${ex.getMessage}. Retrying after ${retryDelay}ms...")

          // Use akka.pattern.after for async delay
          akka.pattern.after(
            FiniteDuration(retryDelay, TimeUnit.MILLISECONDS),
            scheduler
          )(attemptWithRetry(nextSession))
        } else {
          log.error(s"Request failed after ${retry} retries: ${ex.getMessage}")
          // All retries exhausted - reject
          Future.successful(
            session.reject(
              code = -32603,
              message = s"Request failed after ${maxRetry} retries",
              processorName = name,
              details = Some(ex.getMessage)
            )
          )
        }
    }.flatMap { result =>
      val resultRetry = result.getData[Int]("retry").getOrElse(0)
      val resultMaxRetry = result.getData[Int]("maxRetry").getOrElse(maxRetries)
      val resultRetryDelay = result.getData[Long]("retryDelayMs").getOrElse(delayMs)

      // Also check if wrapped processor rejected the session (not an exception)
      if (result.isRejected && resultRetry < resultMaxRetry) {
        val nextSession = result
          .putData("retry", resultRetry + 1)
          .copy(rejection = None) // Clear rejection for retry

        log.warn(s"Request rejected (retry ${resultRetry}/${resultMaxRetry}): ${result.rejection}. Retrying after ${resultRetryDelay}ms...")

        akka.pattern.after(
          FiniteDuration(resultRetryDelay, TimeUnit.MILLISECONDS),
          scheduler
        )(attemptWithRetry(nextSession))
      } else {
        Future.successful(result)
      }
    }
  }
}

object RetryProcessor {
  def apply(
    wrapped: Processor,
    maxRetries: Int = 3,
    delayMs: Long = 1000
  )(implicit ec: ExecutionContext, scheduler: Scheduler): RetryProcessor = {
    new RetryProcessor(wrapped, maxRetries, delayMs)
  }
}
