package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.Logger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpMethods, StatusCodes, HttpEntity, ContentTypes, HttpResponse, HttpHeader}
import akka.http.scaladsl.model.headers.{RawHeader, HttpEncodings}
import akka.http.scaladsl.settings.{ConnectionPoolSettings, ClientConnectionSettings}
import akka.http.scaladsl.coding.Coders

import io.syspulse.ika.processor.{RequestProcessor, Session}
import io.syspulse.ika.store.ProxyData

/**
 * HttpClientProcessor makes the actual HTTP request to the destination.
 *
 * This is a generic HTTP client processor that works with any protocol.
 * Protocol-specific header filtering should be done by upstream processors
 * (e.g., Rpc3Processor for JSON-RPC).
 *
 * Features:
 * - Uses destination from session (set by LoadBalancerProcessor)
 * - Uses request body and headers from session (can be modified by upstream processors)
 * - Respects timeout from session (set by TimeoutProcessor)
 * - Handles compression/decompression (gzip, deflate)
 * - Adds Accept-Encoding header if compression is configured
 * - Sets response on session
 *
 * IMPORTANT: This processor uses session.requestBody and session.requestHeaders,
 * which means upstream processors can modify the request before HTTP is sent.
 * For example, Rpc3Processor filters problematic headers for QuickNode,
 * or a BodyTransformProcessor could modify the request body.
 *
 * This is typically the last processor in the request chain before response processors.
 */
class HttpClientProcessor(
  compression: String = "" // Compression to request (e.g., "gzip, deflate")
)(implicit ec: ExecutionContext, actorSystem: ActorSystem) extends RequestProcessor {

  private val log = Logger(s"${name}")

  def name: String = "HttpClient"

  /**
   * Create connection pool settings with deterministic timeout
   */
  private def createPoolSettings(timeoutMs: Long): ConnectionPoolSettings = {
    val timeout = FiniteDuration(timeoutMs, TimeUnit.MILLISECONDS)
    ConnectionPoolSettings(actorSystem)
      .withBaseConnectionBackoff(timeout)
      .withMaxConnectionBackoff(timeout)
      .withConnectionSettings(
        ClientConnectionSettings(actorSystem)
          .withIdleTimeout(timeout)
          .withConnectingTimeout(timeout)
      )
  }

  /**
   * Decode compressed response
   */
  private def decodeResponse(response: HttpResponse): HttpResponse = {
    log.debug(s"Response: status=${response.status}, encoding=${response.encoding}, contentLength=${response.entity.contentLengthOption}")

    val decoder = response.encoding match {
      case HttpEncodings.gzip =>
        Coders.Gzip
      case HttpEncodings.deflate =>
        Coders.Deflate
      case HttpEncodings.identity =>
        Coders.NoCoding
      case other =>
        log.warn(s"Unknown encoding: $other")
        Coders.NoCoding
    }

    decoder.decodeMessage(response)
  }

  /**
   * Make HTTP POST request
   */
  private def makeRequest(uri: String, body: String, headers: Seq[HttpHeader], timeoutMs: Long): Future[String] = {
    // Add compression header if requested
    // Protocol-specific header filtering should be done by upstream processors (e.g., Rpc3Processor)
    val requestHeaders = headers ++ {
      if (compression.nonEmpty)
        Seq(RawHeader("Accept-Encoding", compression))
      else
        Seq.empty
    }

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uri,
      headers = requestHeaders,
      entity = HttpEntity(ContentTypes.`application/json`, body)
    )

    log.debug(s"HTTP POST request: ${uri}")

    lazy val http = Http()
      .singleRequest(
        request,
        settings = createPoolSettings(timeoutMs)
      )
      .map(decodeResponse)
      .flatMap { res =>
        res.status match {
          case StatusCodes.OK =>
            val bodyFuture = res.entity.dataBytes.runReduce(_ ++ _)
            bodyFuture.map { data =>
              val responseBody = data.utf8String
              log.debug(s"Response body: ${responseBody.take(200)}...")
              responseBody
            }

          case _ =>
            val bodyFuture = res.entity.dataBytes.runReduce(_ ++ _)
            bodyFuture.flatMap { data =>
              val responseBody = data.utf8String
              log.warn(s"HTTP error: ${res.status}: ${responseBody}")
              Future.failed(new Exception(s"${res.status}: ${responseBody}"))
            }
        }
      }
      .recoverWith {
        case e =>
          log.error(s"HTTP request failed: ${uri}: ${e.getMessage}")
          Future.failed(e)
      }

    http
  }

  def processRequest(session: Session): Future[Session] = {
    // Check if response already set (e.g., from cache)
    if (session.responseBody.isDefined) {
      log.debug("Response already set, skipping HTTP request")
      return Future.successful(session)
    }

    // Get destination from processorData
    session.getData[String]("destination") match {
      case Some(uri) =>
        val timeoutMs = session.getData[Long]("timeoutMs").getOrElse(10000L)
        log.info(s"${session.requestBody.take(85)} --> ${uri}")

        // IMPORTANT: Use session.requestBody and session.requestHeaders
        // These can be modified by upstream processors before reaching HTTP
        makeRequest(uri, session.requestBody, session.requestHeaders, timeoutMs)
          .map { responseBody =>
            session.withResponse(responseBody, ProxyData.REMOTE)
          }
          .recoverWith {
            case e: Exception =>
              // Let retry processor handle this
              Future.failed(e)
          }

      case None =>
        log.error("No destination set on session")
        Future.successful(
          session.reject(
            code = -32603,
            message = "No destination set for request",
            processorName = name
          )
        )
    }
  }
}

object HttpClientProcessor {
  def apply()(implicit ec: ExecutionContext, actorSystem: ActorSystem): HttpClientProcessor = {
    new HttpClientProcessor("")
  }

  def apply(compression: String)(implicit ec: ExecutionContext, actorSystem: ActorSystem): HttpClientProcessor = {
    new HttpClientProcessor(compression)
  }
}
