package io.syspulse.ika.processor.impl

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpHeader
import com.typesafe.config.{Config => TypesafeConfig}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{BidirectionalProcessor, Processor, Session}
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * MetaProcessor extracts meta-information from request headers and stores it in Session.
 *
 * - meta headers are NEVER forwarded downstream (they are removed from request headers)
 * - extracted key/value pairs are stored in Session under "meta" (Map[String,String])
 *
 * Supported value syntaxes (comma-separated items):
 * - "k1, k2, ..."         -> keys with empty values (presence)
 * - "k=v, k2 = v2, ..."   -> key/value pairs
 * - "{k}"                 -> treated as key "k" with empty value (presence)
 *
 * If multiple meta headers are configured, values are merged; later headers override keys.
 */
final class MetaProcessor(
  metaHeaders: Seq[String]
)(implicit ec: ExecutionContext) extends BidirectionalProcessor {

  override val name: String = "Meta"
  private val log = Logger(name)

  private val headerNames: Seq[String] =
    metaHeaders.map(_.trim).filter(_.nonEmpty)

  override def processRequest(session: Session): Future[Session] = {
    if (headerNames.isEmpty) return Future.successful(session)

    val extracted: Map[String, String] =
      headerNames.foldLeft(Map.empty[String, String]) { (acc, hn) =>
        session.requestHeaders
          .find(h => h.lowercaseName() == hn.toLowerCase(java.util.Locale.ROOT))
          .map(h => acc ++ parseMetaHeaderValue(h.value()))
          .getOrElse(acc)
      }

    // Remove meta headers from request before forwarding downstream.
    val stripped = headerNames.foldLeft(session) { (s, hn) => s.removeRequestHeader(hn) }

    val updated =
      if (extracted.nonEmpty) stripped.putData("meta", extracted)
      else stripped.removeData("meta")

    if (extracted.nonEmpty) log.debug(s"Meta extracted: $extracted")
    Future.successful(updated)
  }

  override def processResponse(session: Session): Future[Session] =
    Future.successful(session)

  private def parseMetaHeaderValue(v0: String): Map[String, String] = {
    val v = Option(v0).getOrElse("")
    val parts = v.split(",").toSeq.map(_.trim).filter(_.nonEmpty)
    parts.foldLeft(Map.empty[String, String]) { (acc, p) =>
      // key=value
      val eqIdx = p.indexOf('=')
      if (eqIdx >= 0) {
        val k = p.substring(0, eqIdx).trim
        val vv = p.substring(eqIdx + 1).trim
        if (k.nonEmpty) acc + (k -> vv) else acc
      } else if (p.startsWith("{") && p.endsWith("}") && p.length > 2) {
        // {k} -> k=""
        val k = p.substring(1, p.length - 1).trim
        if (k.nonEmpty) acc + (k -> "") else acc
      } else {
        // bare key presence
        acc + (p -> "")
      }
    }
  }
}

object MetaProcessor extends ProcessorConfigurable {
  override val tpe: String = "meta"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    // config:
    // meta_1 { type="meta://"; headers=["x-meta","x-meta-2"] }
    val headers: Seq[String] =
      if (cfg.hasPath("headers")) cfg.getStringList("headers").asScala.toSeq
      else if (cfg.hasPath("metaHeaders")) cfg.getStringList("metaHeaders").asScala.toSeq
      else Seq.empty

    Seq(new MetaProcessor(headers))
  }
}

