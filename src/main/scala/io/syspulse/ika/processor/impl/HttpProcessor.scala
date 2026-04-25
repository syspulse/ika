package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.Logger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, HttpHeader, HttpMethods, HttpRequest, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.model.headers.{RawHeader, HttpEncodings}
import akka.http.scaladsl.settings.{ConnectionPoolSettings, ClientConnectionSettings}
import akka.http.scaladsl.coding.Coders
import akka.util.ByteString

import io.syspulse.ika.processor.{RequestProcessor, Session}
import io.syspulse.ika.processor.ResponseSource
import com.typesafe.config.{Config => TypesafeConfig}
import io.syspulse.ika.processor.Processor
import io.syspulse.ika.processor.util.ProcessorConfigurable
import scala.jdk.CollectionConverters._
import akka.pattern.after
import java.util.concurrent.TimeoutException
import akka.http.scaladsl.model.Uri
import scala.util.{Try, Success, Failure}

/**
 * HttpProcessor makes the actual HTTP request to the destination.
 *
 * This is a generic HTTP client processor that works with any protocol.
 * Protocol-specific header filtering should be done by upstream processors
 * (e.g., Rpc3Processor for JSON-RPC).
 *
 * Features:
 * - Uses destination from session (set by PoolProcessor)
 * - Uses request body and headers from session (can be modified by upstream processors)
 * - Respects timeout from session (set by TimeoutProcessor)
 * - Handles compression/decompression (gzip, deflate)
 * - Adds Accept-Encoding header if compression is configured
 * - Sets response on session
 * - Can apply configured method and headers (from config)
 *
 * IMPORTANT: This processor uses session.requestBody and session.requestHeaders,
 * which means upstream processors can modify the request before HTTP is sent.
 * For example, Rpc3Processor filters problematic headers for QuickNode,
 * or a BodyTransformProcessor could modify the request body.
 *
 * This is typically the last processor in the request chain before response processors.
 */
class HttpProcessor(
  method: Option[String] = None,
  headers: Map[String, String] = Map.empty,
  compression: String = "", // Compression to request (e.g., "gzip, deflate")
  connectTimeoutMs: Option[Long] = None,
  responseTimeoutMs: Option[Long] = None
)(implicit ec: ExecutionContext, actorSystem: ActorSystem) extends RequestProcessor {

  private val log = Logger(s"${name}")

  def name: String = "Http"

  override def toString: String =
    s"$name($method,${headers},$compression,$connectTimeoutMs,$responseTimeoutMs)"

  private case class UpstreamResponse(
    status: StatusCode,
    headers: Seq[HttpHeader],
    body: ByteString,
    contentType: ContentType
  )

  /**
   * Create connection pool settings with deterministic timeout
   */
  private def createPoolSettings(connectTimeoutMs: Long): ConnectionPoolSettings = {
    val timeout = FiniteDuration(connectTimeoutMs, TimeUnit.MILLISECONDS)
    ConnectionPoolSettings(actorSystem)
      .withBaseConnectionBackoff(timeout)
      .withMaxConnectionBackoff(timeout)
      .withConnectionSettings(
        ClientConnectionSettings(actorSystem)
          //.withIdleTimeout(timeout)
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

  private def parseMethod(method: String): akka.http.scaladsl.model.HttpMethod =
    method.toUpperCase match {
      case "GET"  => HttpMethods.GET
      case "POST" => HttpMethods.POST
      case "OPTIONS" => HttpMethods.OPTIONS
      case "HEAD" => HttpMethods.HEAD
      case "PUT" => HttpMethods.PUT
      case "DELETE" => HttpMethods.DELETE
      case "PATCH" => HttpMethods.PATCH
      case "TRACE" => HttpMethods.TRACE
      case "CONNECT" => HttpMethods.CONNECT      
      case other =>
        log.warn(s"Unsupported HTTP method: $other (defaulting to POST)")
        HttpMethods.POST
    }

  private def configuredMethod(): akka.http.scaladsl.model.HttpMethod =
    method.map(parseMethod).getOrElse(HttpMethods.POST)

  private def applyConfiguredHeaders(session: Session): Session = {
    headers.foldLeft(session) { case (s, (k, v)) =>
      val hk = k.trim
      if (hk.isEmpty) s
      else s.addRequestHeader(RawHeader(hk, v))
    }
  }

  private def appendSuffix(base: String, suffix: String): Try[String] =
    HttpProcessor.appendSuffix(base, suffix)

  /**
   * Make HTTP request
   */
  private def makeRequest(
    method: akka.http.scaladsl.model.HttpMethod,
    uri: String,
    body: ByteString,
    headers: Seq[HttpHeader],
    connectTimeoutMs: Long,
    responseTimeoutMs: Long,
    contentType: ContentType
  ): Future[UpstreamResponse] = {
    // Add compression header if requested
    // Protocol-specific header filtering should be done by upstream processors (e.g., Rpc3Processor)
    val requestHeaders = headers ++ {
      if (compression.nonEmpty)
        Seq(RawHeader("Accept-Encoding", compression))
      else
        Seq.empty
    }

    val entity = method match {
      case HttpMethods.GET => HttpEntity.Empty
      case _               => HttpEntity(contentType, body)
    }

    val request = HttpRequest(method = method, uri = uri, headers = requestHeaders, entity = entity)

    log.debug(s"HTTP ${method.value} request: ${uri}, headers=${requestHeaders}, body size=${body.size} bytes")

    val httpF = Http()
      .singleRequest(request, settings = createPoolSettings(connectTimeoutMs))
      .map(decodeResponse)
      .flatMap { res =>
        val bodyFuture = res.entity.dataBytes.runReduce(_ ++ _)
        bodyFuture.map { data =>
          log.debug(s"Response: status=${res.status}, body size=${data.size} bytes")
          UpstreamResponse(
            status = res.status,
            headers = res.headers,
            body = data,
            contentType = res.entity.contentType
          )
        }
      }
      .recoverWith {
        case e =>
          log.error(s"HTTP request failed: ${uri}: ${e.getMessage}")
          Future.failed(e)
      }

    val timeoutF =
      after(FiniteDuration(responseTimeoutMs, TimeUnit.MILLISECONDS), actorSystem.scheduler) {
        Future.failed(new TimeoutException(s"HTTP response timeout: ${responseTimeoutMs} ms"))
      }

    Future.firstCompletedOf(Seq(httpF, timeoutF))
  }

  def processRequest(session: Session): Future[Session] = {
    
    // Check if response already set (e.g., from cache)
    if (session.responseBody.isDefined) {
      log.debug("Response already set, skipping HTTP request")
      return Future.successful(session)
    }

    // Apply configured headers (if any) before sending
    val sessionWithHeaders = applyConfiguredHeaders(session)
    
    // Get destination from processorData
    sessionWithHeaders.getData[String]("destination") match {
      case Some(uri) =>        

        val suffix = sessionWithHeaders.getData[String]("http.uriSuffix").getOrElse("")        
        val already = sessionWithHeaders.getData[Boolean]("http.destinationHasSuffix").getOrElse(false)        
        val finalUriTry =
          if (!already && suffix.nonEmpty) appendSuffix(uri, suffix)
          else Success(uri)

        finalUriTry match {
          case Failure(e) =>
            log.error(s"Failed to append uriSuffix: destination='$uri', uriSuffix='$suffix': ${e.getMessage}", e)
            return Future.successful(
              sessionWithHeaders.reject(
                code = -32603,
                message = s"Failed to append uriSuffix to destination: ${e.getMessage}",
                processorName = name
              )
            )
          case Success(_) =>
        }

        val finalUri = finalUriTry.get

        val timeoutMs = sessionWithHeaders.getData[Long]("timeoutMs").getOrElse(10000L)
        val connectMs = connectTimeoutMs.getOrElse(timeoutMs)
        val responseMs = responseTimeoutMs.getOrElse(timeoutMs)
        val m = method.map(parseMethod)
          .orElse(sessionWithHeaders.getData[String]("http.method").map(parseMethod))
          .getOrElse(HttpMethods.POST)

        val contentType: ContentType = sessionWithHeaders
          .getData[String]("http.contentType")
          .flatMap { ct =>
            ContentType.parse(ct) match {
              case Right(v) => Some(v)
              case Left(err) =>
                log.warn(s"Invalid Content-Type '$ct': ${err.toString}")
                None
            }
          }
          .getOrElse(ContentTypes.`application/json`)
        
        log.debug(s"${sessionWithHeaders} --> ${finalUri}")
        log.info(s"${m.value}(${sessionWithHeaders.requestBody.size} bytes) --> ${finalUri}")

        // IMPORTANT: Use session.requestBody and session.requestHeaders
        // These can be modified by upstream processors before reaching HTTP
        makeRequest(m, finalUri, sessionWithHeaders.requestBody, sessionWithHeaders.requestHeaders, connectMs, responseMs, contentType)
          .map { rsp =>
            val s1 = sessionWithHeaders.withResponse(
              body = rsp.body,
              source = ResponseSource.REMOTE,
              status = rsp.status,
              headers = rsp.headers,
              contentType = rsp.contentType
            )
            // Signal retry processor that some HTTP statuses should be retried (without failing the Future).
            // This keeps default behavior "return exactly what upstream returned" if retries are exhausted.
            if (rsp.status.intValue() >= 500) s1.putData("http.retryable", true)
            else s1.removeData("http.retryable")
          }
          .recoverWith {
            case e: Exception =>
              // Let retry processor handle this
              Future.failed(e)
          }

      case None =>
        log.error("No destination set on session")
        Future.successful(
          sessionWithHeaders.reject(
            code = -32603,
            message = "No destination set for request",
            processorName = name
          )
        )
    }
  }
}

object HttpProcessor extends ProcessorConfigurable {
  override val tpe: String = "http"

  /** Append suffix path+query (like "/v1/x?y=1") to base destination. */
  private[impl] def appendSuffix(base: String, suffix: String): Try[String] = Try {
    val s = Option(suffix).getOrElse("").trim
    if (s.isEmpty || s == "/") base
    else {

      // Parse for query string extraction (suffix may include query).
      val suffixUri = Uri(s)
      val suffixPath = suffixUri.path.toString()
      val suffixQuery = suffixUri.rawQueryString.map(q => s"?$q").getOrElse("")

      // Do string join for path to avoid Path '++' double-slash edge-cases.
      // Also avoid Uri.withRawQueryString(null) (can NPE in Akka HTTP parser).
      val baseNoQueryOrFragment = {
        val iQ = base.indexOf('?')
        val iF = base.indexOf('#')
        val cut =
          if (iQ >= 0 && iF >= 0) math.min(iQ, iF)
          else if (iQ >= 0) iQ
          else if (iF >= 0) iF
          else base.length
        base.substring(0, cut)
      }
      val base0 = if (baseNoQueryOrFragment.endsWith("/")) baseNoQueryOrFragment.dropRight(1) else baseNoQueryOrFragment

      val sp =
        if (suffixPath.isEmpty || suffixPath == "/") ""
        else if (suffixPath.startsWith("/")) suffixPath
        else s"/$suffixPath"

      s"${base0}${sp}${suffixQuery}"
    }
  }

  private def getDurationMsOpt(cfg: TypesafeConfig, path: String): Option[Long] =
    if (!cfg.hasPath(path)) None
    else {
      val v =
        try cfg.getDuration(path).toMillis
        catch { case _: Throwable => cfg.getLong(path) }
      Some(v)
    }

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val method = if (cfg.hasPath("method")) Some(cfg.getString("method")) else None

    val headers =
      if (cfg.hasPath("headers"))
        cfg.getConfig("headers").entrySet().asScala.map(e => e.getKey -> cfg.getString(s"headers.${e.getKey}")).toMap
      else Map.empty[String, String]

    val compression = if (cfg.hasPath("compression")) cfg.getString("compression") else ""

    val connectTimeoutMs = getDurationMsOpt(cfg, "connectTimeout")
    val responseTimeoutMs = getDurationMsOpt(cfg, "responseTimeout")

    Seq(
      new HttpProcessor(
        method = method,
        headers = headers,
        compression = compression,
        connectTimeoutMs = connectTimeoutMs,
        responseTimeoutMs = responseTimeoutMs
      )
    )
  }
}
