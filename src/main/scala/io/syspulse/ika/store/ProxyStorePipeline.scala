package io.syspulse.ika.store

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpHeader, HttpMethod}
import akka.util.ByteString

import io.syspulse.ika.Config
import io.syspulse.ika.processor.{Session, ProcessorPipeline}
import io.syspulse.ika.processor.util.ProcessorPipelineBuilder
import com.typesafe.config.ConfigFactory

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
  val profile: String
)(implicit ec: ExecutionContext) extends ProxyStore {

  private val log = Logger(s"${this}")

  log.info(s"ProxyStorePipeline initialized with profile: $profile")
  log.info(s"Pipeline: ${pipeline}")

  /**
   * process request
   */
  def proxy(method: HttpMethod, uriSuffix: String, req: ByteString, headers: Seq[HttpHeader]): Future[Session] = {
    log.debug(s"Processing request: ${req.take(100).size} bytes...")

    // Create initial session
    val session = Session(
      requestBody = req,
      requestHeaders = headers
    ).putData("http.method", method.value)
      .putData("http.uriSuffix", uriSuffix)

    // Execute pipeline
    pipeline.process(session).map { resultSession =>
      val cacheHit = resultSession.getData[Boolean]("cacheHit").getOrElse(false)
      log.debug(s"Request completed. Source: ${resultSession.responseSource}, Cache hit: ${cacheHit}, Rejected: ${resultSession.isRejected}, Duration: ${resultSession.durationMs}ms")
      resultSession
    }.recover { case ex: Exception =>
      log.error(s"Pipeline execution failed: ${ex.getMessage}", ex)
      Session(requestBody = req, requestHeaders = headers)
        .reject(code = -32603, message = s"Pipeline error: ${ex.getMessage}", processorName = "ProxyStorePipeline")
        .withResponse(ByteString(s"""{"jsonrpc": "2.0", "error": {"code": -32603, "message": "Pipeline error: ${ex.getMessage}"}, "id": null}"""))
    }
  }

  override def toString: String = s"ProxyStorePipeline($profile)"
}

object ProxyStorePipeline {
  /**
   * Create a ProxyStorePipeline with default profile.
   */
  def apply()(implicit config: Config, ec: ExecutionContext, actorSystem: ActorSystem): ProxyStorePipeline = {
    apply("proxy")
  }

  /**
   * Create a ProxyStorePipeline with a specific profile
   */
  def apply(profile: String)(implicit config: Config, ec: ExecutionContext, actorSystem: ActorSystem): ProxyStorePipeline = {
    // Profiles are user-defined in application.conf under `profile.<name>`
    val cfg = ConfigFactory.load().resolve()
    val pipeline = ProcessorPipelineBuilder.fromProfile(cfg, profile)(ec, actorSystem)
    new ProxyStorePipeline(pipeline, profile)
  }

  /**
   * Create a ProxyStorePipeline with a custom pipeline
   */
  def apply(pipeline: ProcessorPipeline, profile: String = "custom")(implicit ec: ExecutionContext): ProxyStorePipeline = {
    new ProxyStorePipeline(pipeline, profile)
  }
}
