package io.syspulse.ika.store

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpHeader

import io.syspulse.ika.Config
import io.syspulse.ika.processor.{Session, ProcessorPipeline}
import io.syspulse.ika.processor.util.ProcessorConfig
import io.syspulse.ika.processor.util.ProcessorPipelineBuilder

/**
 * ProxyStorePipeline implements the ProxyStore trait using the processor pipeline architecture.
 *
 * This is the main integration point that:
 * 1. Receives RPC requests via the rpc() method
 * 2. Creates a Session from the request
 * 3. Executes the configured pipeline
 * 4. Extracts the response from the Session
 * 5. Returns ProxyData to the HTTP routes
 *
 * The pipeline is built by PipelineBuilder based on configuration.
 */
class ProxyStorePipeline(
  val pipeline: ProcessorPipeline,
  val profile: String = "web3"
)(implicit ec: ExecutionContext) extends ProxyStore {

  private val log = Logger(s"${this}")

  log.info(s"ProxyStorePipeline initialized with profile: $profile")
  log.info(s"Pipeline: ${pipeline}")

  /**
   * process request
   */
  def proxy(req: String, headers: Seq[HttpHeader]): Future[ProxyData] = {
    log.debug(s"Processing request: ${req.take(100)}...")

    // Create initial session
    val session = Session(
      requestBody = req,
      requestHeaders = headers
    )

    // Execute pipeline
    pipeline.process(session).map { resultSession =>
      // Extract response from session
      // Note: RejectionProcessor in the pipeline has already converted rejections to response body
      resultSession.responseBody match {
        case Some(body) =>
          val cacheHit = resultSession.getData[Boolean]("cacheHit").getOrElse(false)
          val httpStatus = resultSession.getData[Int]("httpStatusCode")

          if (resultSession.isRejected) {
            val rejection = resultSession.rejection.get
            log.warn(s"Request rejected by ${rejection.processorName}: ${rejection.message}, HTTP status: ${httpStatus.getOrElse("default")}")
          } else {
            log.debug(s"Request completed successfully. Source: ${resultSession.responseSource}, Cache hit: ${cacheHit}, Duration: ${resultSession.durationMs}ms")
          }

          // Note: httpStatusCode in processorData could be used by routes to set HTTP response status
          // For now, ProxyData doesn't support it, but it's available in the session
          ProxyData(
            body = body,
            src = resultSession.responseSource
          )

        case None =>
          // This shouldn't happen if pipeline is configured correctly (RejectionProcessor should set response)
          log.error("Pipeline completed but no response body set")
          ProxyData(
            body = """{"jsonrpc": "2.0", "error": {"code": -32603, "message": "No response from pipeline"}, "id": null}""",
            src = ProxyData.LOCAL
          )
      }
    }.recover {
      case ex: Exception =>
        // Unhandled exception (shouldn't happen if RetryProcessor and RejectionProcessor are in pipeline)
        log.error(s"Pipeline execution failed: ${ex.getMessage}", ex)
        ProxyData(
          body = s"""{"jsonrpc": "2.0", "error": {"code": -32603, "message": "Pipeline error: ${ex.getMessage}"}, "id": null}""",
          src = ProxyData.LOCAL
        )
    }
  }

  override def toString: String = s"ProxyStorePipeline($profile)"
}

object ProxyStorePipeline {
  /**
   * Create a ProxyStorePipeline with the default web3 profile
   */
  def apply()(implicit config: Config, ec: ExecutionContext, actorSystem: ActorSystem): ProxyStorePipeline = {
    apply("web3")
  }

  /**
   * Create a ProxyStorePipeline with a specific profile
   */
  def apply(profile: String)(implicit config: Config, ec: ExecutionContext, actorSystem: ActorSystem): ProxyStorePipeline = {
    val builder = ProcessorPipelineBuilder(
      destinations = config.destinations,
      processorConfig = ProcessorConfig.default,
      poolStrategy = "sticky",
      cacheUri = "rpc3://"  // TODO: get from config
    )(ec, actorSystem)
    val pipeline = builder.build(profile)
    new ProxyStorePipeline(pipeline, profile)
  }

  /**
   * Create a ProxyStorePipeline with a custom pipeline
   */
  def apply(pipeline: ProcessorPipeline, profile: String = "custom")(implicit ec: ExecutionContext): ProxyStorePipeline = {
    new ProxyStorePipeline(pipeline, profile)
  }
}
