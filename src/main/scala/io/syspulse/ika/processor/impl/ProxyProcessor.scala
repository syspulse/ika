package io.syspulse.ika.processor.impl

import java.util.Locale
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.RawHeader
import com.typesafe.config.{Config => TypesafeConfig}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{BidirectionalProcessor, Processor, Session}
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * ProxyProcessor implements HTTP/1.1 proxy behaviour (RFC 7230 §6.1).
 *
 * On every **request**:
 *  1. Sets `destination` in the session from the `Host` header when no destination
 *     has been placed there yet (e.g. when there is no PoolProcessor upstream).
 *     The value becomes `<scheme>://<host>` which HttpProcessor uses as the base URL;
 *     the path / query are taken from `http.uriSuffix` (set by ProxyStorePipeline).
 *  2. Strips RFC-7230 hop-by-hop headers — these MUST NOT be forwarded by a proxy:
 *       Connection, Keep-Alive, Proxy-Authenticate, Proxy-Authorization,
 *       TE, Trailer, Transfer-Encoding, Upgrade, Proxy-Connection (de-facto standard)
 *  3. Strips any headers whose names appear in the `Connection` header value
 *     (dynamic hop-by-hop headers, RFC 7230 §6.1).
 *  4. Optionally appends a `Via` header (RFC 7230 §5.7.1).
 *
 * On every **response**:
 *  - Strips the same hop-by-hop set (including dynamic ones) from the response headers.
 *
 * Configuration example:
 * {{{
 * proxy_1 {
 *   type = "proxy"
 *   scheme = "https"          # default "http"
 *   addVia = true             # default true
 *   stripRequest  = ["X-Internal-Token"]   # additional request headers to strip
 *   stripResponse = ["X-Backend-Id"]       # additional response headers to strip
 * }
 * }}}
 *
 * Typical pipeline positions:
 * {{{
 *   # Forward proxy (destination from Host header):
 *   ProxyProcessor → RetryProcessor → HttpProcessor
 *
 *   # Load-balanced proxy (destination from Pool, headers still need cleaning):
 *   ProxyProcessor → PoolProcessor → RetryProcessor → HttpProcessor
 * }}}
 */
class ProxyProcessor(
  scheme: String = "http",
  addVia: Boolean = true,
  viaValue: String = "1.1 ika-proxy",
  extraStripRequest: Set[String] = Set.empty,
  extraStripResponse: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends BidirectionalProcessor {

  override val name: String = "Proxy"

  private val log = Logger(name)

  // RFC 7230 §6.1 permanent hop-by-hop headers (lower-cased)
  private val HOP_BY_HOP: Set[String] = Set(
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    // De-facto standard non-standard header, treated the same way
    "proxy-connection"
  )

  private val extraReqL: Set[String]  = extraStripRequest.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty)
  private val extraResL: Set[String]  = extraStripResponse.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty)

  // Names that appear in the `Connection` header value are also hop-by-hop (RFC 7230 §6.1)
  private def dynamicHopByHop(connectionValue: Option[String]): Set[String] =
    connectionValue
      .map(_.split(',').map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)

  private def stripHeaders(session: Session, names: Set[String], response: Boolean): Session =
    names.foldLeft(session) { (s, n) =>
      if (response) s.removeResponseHeader(n) else s.removeRequestHeader(n)
    }

  override def processRequest(session: Session): Future[Session] = {
    // ── 1. Destination from Host header ────────────────────────────────────
    val s1 = if (session.getData[String]("destination").isEmpty) {
      session.requestHeaderMap.get("host") match {
        case Some(h) =>
          val host = h.value.trim
          val dest = if (host.startsWith("http://") || host.startsWith("https://")) host
                     else s"$scheme://$host"
          log.debug(s"Destination from Host header: '$dest'")
          session.putData("destination", dest)
        case None =>
          log.debug("No Host header — destination must be set by an upstream processor (e.g. PoolProcessor)")
          session
      }
    } else session

    // ── 2. Collect dynamic hop-by-hop names from Connection header ─────────
    val dynHbh = dynamicHopByHop(s1.requestHeaderMap.get("connection").map(_.value))
    val toStrip = HOP_BY_HOP ++ dynHbh ++ extraReqL

    // ── 3. Strip ───────────────────────────────────────────────────────────
    val s2 = stripHeaders(s1, toStrip, response = false)

    // ── 4. Via ─────────────────────────────────────────────────────────────
    val s3 = if (addVia) {
      val existing = s2.requestHeaderMap.get("via").map(_.value + ", ").getOrElse("")
      s2.removeRequestHeader("via").addRequestHeader(RawHeader("Via", s"${existing}${viaValue}"))
    } else s2

    log.trace(s"Request stripped: $toStrip")
    Future.successful(s3)
  }

  override def processResponse(session: Session): Future[Session] = {
    val dynHbh = dynamicHopByHop(session.responseHeaderMap.get("connection").map(_.value))
    val toStrip = HOP_BY_HOP ++ dynHbh ++ extraResL
    val s1 = stripHeaders(session, toStrip, response = true)
    log.trace(s"Response stripped: $toStrip")
    Future.successful(s1)
  }

  override def toString: String =
    s"$name($scheme, via=$addVia, viaValue='$viaValue', stripReq=$extraStripRequest, stripRes=$extraStripResponse)"
}

object ProxyProcessor extends ProcessorConfigurable {
  override val tpe: String = "proxy"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val scheme   = if (cfg.hasPath("scheme"))    cfg.getString("scheme")        else "http"
    val addVia   = if (cfg.hasPath("addVia"))    cfg.getBoolean("addVia")       else true
    val viaValue = if (cfg.hasPath("viaValue"))  cfg.getString("viaValue")      else "1.1 ika-proxy"

    def strSet(path: String): Set[String] =
      if (cfg.hasPath(path)) cfg.getStringList(path).asScala.toSet else Set.empty

    val extraReq = strSet("stripRequest")
    val extraRes = strSet("stripResponse")

    Seq(new ProxyProcessor(scheme = scheme, addVia = addVia, viaValue = viaValue, extraStripRequest = extraReq, extraStripResponse = extraRes))
  }
}
