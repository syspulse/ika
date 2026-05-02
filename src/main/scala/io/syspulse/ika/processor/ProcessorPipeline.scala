package io.syspulse.ika.processor

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import io.syspulse.ika.telemetry.Telemetry

/**
 * ProcessorPipeline chains multiple processors together sequentially.
 *
 * The pipeline:
 * 1. Executes processors in order
 * 2. Short-circuits based on session state:
 *    - REJECT: Stops processing with error
 *    - RETURN: Stops processing successfully (e.g., cache hit)
 *    - CONTINUE: Continues to next processor
 * 3. Recovers from failures and converts to rejections
 * 4. Logs processor execution
 *
 * Example pipeline:
 * Auth → Logging → Throttle → Cache(RETURN if hit) → LoadBalancer → HTTP → Logging
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
    log.trace(s"Start: ${processors}")

    val sessionWithTelemetry = telemetry match {
      case Some(t) => session.putData("telemetry", t)
      case None    => session
    }

    telemetry.foreach { t =>
      t.incRequests()
      t.addRequestBytes(sessionWithTelemetry.requestBody.length.toLong)
    }

    val initialized = sessionWithTelemetry.withPipeline(processors)

    val start: Future[Session] =
      initialized.nextProcessor match {
        case Some(p) =>
          p.process(initialized).recover { case ex: Exception =>
            initialized.reject(
              code = -32603,
              message = s"Internal processor error: ${ex.getMessage}",
              processorName = p.name,
              details = Some(ex.getClass.getSimpleName)
            )
          }
        case None =>
          Future.successful(initialized)
      }

    start.map { finalSession =>
      log.trace(s"Completed: ${finalSession}: ${finalSession.durationMs}ms")
      telemetry.foreach { t =>
        if (finalSession.isRejected) t.incRejections() else t.incResponses()
        t.addResponseBytes(finalSession.responseBody.map(_.length.toLong).getOrElse(0L))
        t.recordRequestTime(finalSession.durationMs)
      }
      finalSession.complete()
    }
  }

  override def toString: String = {
    s"$name(${processors.map(p => p.toString).mkString(" -> ")})"
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
  def fromSeq(processors: Seq[Processor], name: String = "Pipeline", telemetry: Option[Telemetry] = None)(implicit ec: ExecutionContext): ProcessorPipeline = {
    new ProcessorPipeline(processors, name, telemetry)
  }

  /**
   * Create a pipeline with telemetry
   */
  def withTelemetry(processors: Seq[Processor], name: String, telemetry: Telemetry)(implicit ec: ExecutionContext): ProcessorPipeline = {
    new ProcessorPipeline(processors, name, Some(telemetry))
  }
}
