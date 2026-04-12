package io.syspulse.ika.processor

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import io.syspulse.ika.telemetry.Telemetry

/**
 * ProcessorPipeline chains multiple processors together sequentially.
 *
 * The pipeline:
 * 1. Executes processors in order
 * 2. Short-circuits on rejection (skips remaining processors)
 * 3. Recovers from failures and converts to rejections
 * 4. Logs processor execution
 *
 * Example pipeline:
 * Auth → Logging → Throttle → Cache → LoadBalancer → HTTP → Logging
 */
class ProcessorPipeline(
  val processors: Seq[Processor],
  val name: String = "Pipeline",
  val telemetry: Option[Telemetry] = None
)(implicit ec: ExecutionContext) extends Processor {

  private val log = Logger(s"${name}")

  /**
   * Execute the pipeline by folding over processors.
   * Short-circuits if any processor rejects the session.
   * Records metrics to telemetry.
   */
  def process(session: Session): Future[Session] = {
    log.debug(s"Starting pipeline with ${processors.size} processors")

    // Inject telemetry into session for processors to use
    val sessionWithTelemetry = telemetry match {
      case Some(t) => session.putData("telemetry", t)
      case None => session
    }

    // Record pipeline start
    telemetry.foreach(_.incRequests())

    processors.foldLeft(Future.successful(sessionWithTelemetry)) { (futureSession, processor) =>
      futureSession.flatMap { currentSession =>
        if (currentSession.isRejected) {
          log.debug(s"Skipping processor '${processor.name}' - session already rejected")
          Future.successful(currentSession)
        } else {
          log.debug(s"Executing processor: ${processor.name}")

          processor.process(currentSession)
            .map { resultSession =>
              if (resultSession.isRejected) {
                log.warn(s"Processor '${processor.name}' rejected session: ${resultSession.rejection}")
              } else {
                log.debug(s"Processor '${processor.name}' completed successfully")
              }
              resultSession
            }
            .recoverWith { case ex: Exception =>
              log.error(s"Processor '${processor.name}' failed with exception: ${ex.getMessage}", ex)
              Future.successful(
                currentSession.reject(
                  code = -32603,
                  message = s"Internal processor error: ${ex.getMessage}",
                  processorName = processor.name,
                  details = Some(ex.getClass.getSimpleName)
                )
              )
            }
        }
      }
    }.map { finalSession =>
      log.debug(s"Pipeline completed. Rejected: ${finalSession.isRejected}, Duration: ${finalSession.durationMs}ms")

      // Record metrics
      telemetry.foreach { t =>
        if (finalSession.isRejected) {
          t.incRejections()
        } else {
          t.incResponses()
        }
        t.recordRequestTime(finalSession.durationMs)
      }

      finalSession.complete()
    }
  }

  override def toString: String = {
    s"$name(${processors.map(_.name).mkString(" → ")})"
  }
}

object ProcessorPipeline {
  /**
   * Create a pipeline from a sequence of processors
   */
  def apply(processors: Processor*)(implicit ec: ExecutionContext): ProcessorPipeline = {
    new ProcessorPipeline(processors, "Pipeline", None)
  }

  /**
   * Create a named pipeline from a sequence of processors
   */
  def apply(name: String, processors: Processor*)(implicit ec: ExecutionContext): ProcessorPipeline = {
    new ProcessorPipeline(processors, name, None)
  }

  /**
   * Create a pipeline from a sequence
   */
  def fromSeq(processors: Seq[Processor], name: String = "Pipeline")(implicit ec: ExecutionContext): ProcessorPipeline = {
    new ProcessorPipeline(processors, name, None)
  }

  /**
   * Create a pipeline with telemetry
   */
  def withTelemetry(processors: Seq[Processor], name: String, telemetry: Telemetry)(implicit ec: ExecutionContext): ProcessorPipeline = {
    new ProcessorPipeline(processors, name, Some(telemetry))
  }
}
