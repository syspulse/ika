package io.syspulse.ika.store

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpHeader, HttpMethod, StatusCode, StatusCodes}
import akka.util.ByteString

import io.syspulse.ika.Config
import io.syspulse.ika.processor.{Rejection, Session, ProcessorPipeline}
import io.syspulse.ika.processor.ResponseSource
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
  
  log.info(s"Pipeline: ${profile}: ${pipeline}")

  private def jsonEscape(s: String): String =
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c if c.isControl => f"\\u${c.toInt}%04x"
      case c => c.toString
    }

  private def statusFor(rejection: Rejection): StatusCode = {
    val reason = s"${rejection.message} ${rejection.details.getOrElse("")}".toLowerCase
    if (reason.contains("timeout")) StatusCodes.GatewayTimeout
    else if (rejection.processorName == "Http" || rejection.processorName == "Pool" || rejection.processorName == "Retry") StatusCodes.BadGateway
    else {
      rejection.code match {
        case code if code >= 400 && code < 600 => StatusCode.int2StatusCode(code)
        case -32700 | -32600 | -32602 => StatusCodes.BadRequest
        case -32601 => StatusCodes.NotFound
        case _ => StatusCodes.InternalServerError
      }
    }
  }

  private def isTransportFailure(rejection: Rejection): Boolean =
    rejection.processorName == "Http" ||
      rejection.processorName == "Pool" ||
      rejection.processorName == "Retry"

  private def rejectionBody(rejection: Rejection, status: StatusCode): ByteString = {
    val details = rejection.details.map(d => s""","details":"${jsonEscape(d)}"""").getOrElse("")
    ByteString(
      s"""{"error":"upstream_failed","status":${status.intValue()},"message":"${jsonEscape(rejection.message)}","processor":"${jsonEscape(rejection.processorName)}"${details}}"""
    )
  }

  private def withFallbackErrorResponse(session: Session): Session = {
    if (session.isStreaming) return session
    session.rejection match {
      case Some(rejection) if session.responseBody.forall(_.isEmpty) || (isTransportFailure(rejection) && session.responseStatus.isSuccess()) =>
        val status = statusFor(rejection)
        session.withResponse(
          body = rejectionBody(rejection, status),
          source = ResponseSource.LOCAL,
          status = status,
          contentType = ContentTypes.`application/json`
        )
      case _ =>
        session
    }
  }

  /**
   * process request
   */
  def proxy(method: HttpMethod, uriSuffix: String, req: ByteString, headers: Seq[HttpHeader]): Future[Session] = {
    // Create initial session
    val session = Session(
      requestBody = req,
      requestHeaders = headers
    ).putData("http.method", method.value)
      .putData("http.uriSuffix", uriSuffix)

    // Execute pipeline
    pipeline.process(session).map { resultSession =>
      val cacheHit = resultSession.getData[Boolean]("cacheHit").getOrElse(false)
      val finalSession = withFallbackErrorResponse(resultSession)
      log.debug(s"Pipeline: ${profile}: Data=${finalSession.responseBody.map(_.size).getOrElse(0)}, Cache=${cacheHit}, Rejected=${finalSession.isRejected}, Duration=${finalSession.durationMs}ms")
      finalSession
    }.recover { case ex: Exception =>
      log.error(s"Pipeline execution failed", ex)
      withFallbackErrorResponse(
        Session(requestBody = req, requestHeaders = headers)
          .reject(code = -32603, message = s"Pipeline error: ${ex.getMessage}", processorName = "ProxyStorePipeline", details = Some(ex.getClass.getSimpleName))
      )
    }
  }

  override def toString: String = s"ProxyStorePipeline($profile)"
}

object ProxyStorePipeline {
  /**
   * Create a ProxyStorePipeline with default profile.
   */
  def apply()(implicit config: Config, ec: ExecutionContext, actorSystem: ActorSystem, telemetry: io.syspulse.ika.telemetry.Telemetry): ProxyStorePipeline = {
    apply("proxy")
  }

  /**
   * Create a ProxyStorePipeline with a specific profile
   */
  def apply(profile: String)(implicit config: Config, ec: ExecutionContext, actorSystem: ActorSystem, telemetry: io.syspulse.ika.telemetry.Telemetry): ProxyStorePipeline = {
    // Profiles are user-defined in application.conf under `profile.<name>`
    val cfg = ConfigFactory.load().resolve()
    val pipeline = ProcessorPipelineBuilder.fromProfile(cfg, profile, Some(telemetry))(ec, actorSystem)
    new ProxyStorePipeline(pipeline, profile)
  }

  /**
   * Create a ProxyStorePipeline with a custom pipeline
   */
  def apply(pipeline: ProcessorPipeline, profile: String = "custom")(implicit ec: ExecutionContext): ProxyStorePipeline = {
    new ProxyStorePipeline(pipeline, profile)
  }
}
