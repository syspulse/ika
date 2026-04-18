package io.syspulse.ika.processor.impl

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.RawHeader
import com.typesafe.config.{Config => TypesafeConfig}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{BidirectionalProcessor, Processor, Session}
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * HeaderProcessor adds/removes HTTP headers on request and/or response.
 *
 * It is intentionally protocol-agnostic and should be used for:
 * - Removing internal headers before sending to upstream providers
 * - Adding required content-type / auth headers
 * - Normalizing response headers (optional)
 *
 * Notes:
 * - Header names are matched case-insensitively (Session stores headers keyed by lower-cased name)
 * - Adds overwrite existing headers with the same field name
 */
final class HeaderProcessor(
  removeRequest: Set[String] = Set.empty,
  addRequest: Map[String, String] = Map.empty,
  removeResponse: Set[String] = Set.empty,
  addResponse: Map[String, String] = Map.empty,
  onResponse: Boolean = false
)(implicit ec: ExecutionContext)
    extends BidirectionalProcessor {

  override val name: String = "Header"

  private val log = Logger(name)

  private val removeReqL: Set[String] = removeRequest.map(_.trim).filter(_.nonEmpty).map(_.toLowerCase(java.util.Locale.ROOT))
  private val removeResL: Set[String] = removeResponse.map(_.trim).filter(_.nonEmpty).map(_.toLowerCase(java.util.Locale.ROOT))

  private def applyAdds(session: Session, adds: Map[String, String], isResponse: Boolean): Session = {
    adds.foldLeft(session) { case (s, (k0, v)) =>
      val k = k0.trim
      if (k.isEmpty) s
      else {
        // Akka HTTP treats Content-Type as an entity attribute (not a regular header)
        if (!isResponse && k.equalsIgnoreCase("Content-Type")) {
          s.putData("http.contentType", v)
        } else {
          val h = RawHeader(k, v)
          if (isResponse) s.addResponseHeader(h) else s.addRequestHeader(h)
        }
      }
    }
  }

  private def applyRemoves(session: Session, removesLower: Set[String], isResponse: Boolean): Session = {
    if (removesLower.isEmpty) return session

    val headers = if (isResponse) session.responseHeaders else session.requestHeaders
    val originalCount = headers.size

    val updated = headers.foldLeft(session) { (s, h) =>
      val hn = h.name.toLowerCase(java.util.Locale.ROOT)
      if (removesLower.contains(hn)) {
        log.debug(s"Removing ${if (isResponse) "response" else "request"} header: ${h.name}")
        if (isResponse) s.removeResponseHeader(h.name) else s.removeRequestHeader(h.name)
      } else s
    }
    
    if (!isResponse && removesLower.contains("content-type")) updated.removeData("http.contentType")
    else updated
  }

  override def processRequest(session: Session): Future[Session] = {
    val s1 = applyRemoves(session, removeReqL, isResponse = false)
    val s2 = applyAdds(s1, addRequest, isResponse = false)
    Future.successful(s2)
  }

  override def processResponse(session: Session): Future[Session] = {
    if (!onResponse) return Future.successful(session)

    val s1 = applyRemoves(session, removeResL, isResponse = true)
    val s2 = applyAdds(s1, addResponse, isResponse = true)
    Future.successful(s2)
  }

  override def toString: String =
    s"$name(${removeRequest}, ${addRequest}, ${removeResponse}, ${addResponse})"
}

object HeaderProcessor extends ProcessorConfigurable {
  override val tpe: String = "header"

  private def getStringSeq(cfg: TypesafeConfig, path: String): Seq[String] =
    if (cfg.hasPath(path)) cfg.getStringList(path).asScala.toSeq else Seq.empty

  private def getStringMap(cfg: TypesafeConfig, path: String): Map[String, String] =
    if (!cfg.hasPath(path)) Map.empty
    else cfg.getConfig(path).entrySet().asScala.map(e => e.getKey -> cfg.getString(s"$path.${e.getKey}")).toMap

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val remove = getStringSeq(cfg, "remove").toSet ++ getStringSeq(cfg, "removeRequest").toSet
    val add = getStringMap(cfg, "add") ++ getStringMap(cfg, "addRequest")

    val onResponse = if (cfg.hasPath("onResponse")) cfg.getBoolean("onResponse") else false
    val removeResponse = getStringSeq(cfg, "removeResponse").toSet
    val addResponse = getStringMap(cfg, "addResponse")

    Seq(new HeaderProcessor(removeRequest = remove, addRequest = add, removeResponse = removeResponse, addResponse = addResponse, onResponse = onResponse))
  }
}

