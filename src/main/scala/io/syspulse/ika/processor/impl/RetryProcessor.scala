package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.Logger
import akka.actor.Scheduler
import akka.actor.ActorSystem
import com.typesafe.config.{Config => TypesafeConfig}
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}

import io.syspulse.ika.processor.{Processor, Session}
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * RetryProcessor retries the downstream pipeline segment on failure.
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
  maxRetries: Int,
  delayMs: Long
)(implicit ec: ExecutionContext, scheduler: Scheduler) extends Processor {

  private val log = Logger(s"${name}")

  def name: String = "Retry"

  override def toString: String = s"${name}($maxRetries,${delayMs})"

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

    // The cursor currently points at this processor. Downstream starts at cursor+1.
    val baseCursor = session.cursor.get()
    attemptWithRetry(sessionWithRetry, baseCursor)
  }

  /**
   * Attempt to execute the wrapped processor with retry logic
   */
  private def attemptWithRetry(session: Session, baseCursor: Int): Future[Session] = {
    val retry = session.getData[Int]("retry").getOrElse(0)
    val maxRetry = session.getData[Int]("maxRetry").getOrElse(maxRetries)
    val retryDelay = session.getData[Long]("retryDelayMs").getOrElse(delayMs)

    log.debug(s"Attempt: ${retry}/${maxRetry}")

    val attemptSession = session
      .withCursor(baseCursor) // reset so next() starts from downstream again
      .copy(state = io.syspulse.ika.processor.SessionState.CONTINUE, rejection = None)

    next(attemptSession).recoverWith {
      case ex: Exception =>
        if (retry < maxRetry) {
          val nextSession = session
            .putData("retry", retry + 1)
            .putData("errorReason", s"${ex.getClass.getSimpleName}: ${ex.getMessage}")

          log.warn(s"Failed: retry=${retry}/${maxRetry}: ${ex.getMessage}. Retrying after ${retryDelay}...")

          // Use akka.pattern.after for async delay
          akka.pattern.after(
            FiniteDuration(retryDelay, TimeUnit.MILLISECONDS),
            scheduler
          )(attemptWithRetry(nextSession, baseCursor))
        } else {
          log.error(s"Failed: retry=${retry}/${maxRetry}: ${ex.getMessage}")
          // All retries exhausted - reject
          Future.successful(
            session.reject(
              code = -32603,
              message = s"Failed after: ${retry}/${maxRetry}",
              processorName = name,
              details = Some(ex.getMessage)
            )
          )
        }
    }.flatMap { result =>
      val resultRetry = result.getData[Int]("retry").getOrElse(0)
      val resultMaxRetry = result.getData[Int]("maxRetry").getOrElse(maxRetries)
      val resultRetryDelay = result.getData[Long]("retryDelayMs").getOrElse(delayMs)
      val retryableHttpStatus = result.getData[Boolean]("http.retryable").getOrElse(false)

      // Also check if downstream rejected the session (not an exception)
      if (result.isRejected && resultRetry < resultMaxRetry) {
        val nextSession = result
          .putData("retry", resultRetry + 1)
          .copy(rejection = None) // Clear rejection for retry

        log.warn(s"Rejected (retry ${resultRetry}/${resultMaxRetry}): ${result.rejection}. Retrying after ${resultRetryDelay}ms...")

        akka.pattern.after(
          FiniteDuration(resultRetryDelay, TimeUnit.MILLISECONDS),
          scheduler
        )(attemptWithRetry(nextSession, baseCursor))
      } else if (!result.isRejected && retryableHttpStatus && resultRetry < resultMaxRetry) {
        // Retry on some HTTP statuses without failing the future (e.g., 5xx).
        // Clear response so downstream processors re-run and HttpProcessor will be invoked again.
        val cleared = result.copy(
          responseBody = None,
          responseHeaderMap = Map.empty,
          responseStatus = StatusCodes.OK,
          responseContentType = ContentTypes.`application/json`
        ).removeData("http.retryable")
          .putData("retry", resultRetry + 1)
          .putData("errorReason", s"HTTP status retryable: ${result.responseStatus}")

        log.warn(s"Retryable HTTP status (retry ${resultRetry}/${resultMaxRetry}): ${result.responseStatus}. Retrying after ${resultRetryDelay}ms...")

        akka.pattern.after(
          FiniteDuration(resultRetryDelay, TimeUnit.MILLISECONDS),
          scheduler
        )(attemptWithRetry(cleared, baseCursor))
      } else {
        Future.successful(result)
      }
    }
  }
}

object RetryProcessor {
  def apply(
    maxRetries: Int = 3,
    delayMs: Long = 1000
  )(implicit ec: ExecutionContext, scheduler: Scheduler): RetryProcessor = {
    new RetryProcessor(maxRetries, delayMs)
  }
}

object RetryProcessorConfig extends ProcessorConfigurable {
  override val tpe: String = "retry"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler

    val maxRetries =
      if (cfg.hasPath("maxRetries")) cfg.getInt("maxRetries")
      else if (cfg.hasPath("retries")) cfg.getInt("retries")
      else 3

    val delayMs =
      if (cfg.hasPath("delayMs")) cfg.getLong("delayMs")
      else if (cfg.hasPath("delay")) cfg.getLong("delay")
      else 1000L

    Seq(new RetryProcessor(maxRetries = maxRetries, delayMs = delayMs))
  }
}
