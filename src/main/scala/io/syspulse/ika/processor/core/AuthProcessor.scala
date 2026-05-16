package io.syspulse.ika.processor.core

import scala.concurrent.{ExecutionContext, Future}

import akka.actor.ActorSystem
import com.typesafe.config.{Config => TypesafeConfig}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{BidirectionalProcessor, Processor, Session}
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * AuthProcessor authorizes incoming requests and strips auth headers before forwarding upstream.
 *
 * It strips auth headers directly from [[Session]] for efficiency.
 *
 * Supported strategies:
 * - bearer: requires request header "Authorization: Bearer {secret}"
 * - header: requires request header "<name>: {secret}" (name configured under header.name)
 *
 * Secrets are configured under the per-strategy subtree:
 * bearer { secret = "..." }
 * header { secret = "...", name = "x-api-key" }
 */
sealed trait AuthStrategy {
  def id: String
  def verify(session: Session): Either[String, Session]
  def strip(session: Session): Session
}

object AuthStrategy {
  def fromConfig(cfg: TypesafeConfig, root: TypesafeConfig)(implicit ec: ExecutionContext): AuthStrategy = {
    val rawStrategy = if (cfg.hasPath("strategy")) cfg.getString("strategy") else "bearer"
    val s = Option(rawStrategy).getOrElse("").trim.toLowerCase(java.util.Locale.ROOT)

    s match {
      case "bearer" => BearerStrategy.fromConfig(if (cfg.hasPath("bearer")) cfg.getConfig("bearer") else cfg)
      case "header" => HeaderStrategy.fromConfig(if (cfg.hasPath("header")) cfg.getConfig("header") else cfg)
      case other => UnknownStrategy(other)
    }
  }

  val DEF_ERR_REASON: String = "invalid secret"
}

final case class BearerStrategy(secret: String)(implicit ec: ExecutionContext) extends AuthStrategy {
  override val id: String = "bearer"
  private val headerName = "Authorization"

  override def verify(session: Session): Either[String, Session] = {
    val expected = s"Bearer ${Option(secret).getOrElse("")}"
    val got = session.requestHeaders.find(_.lowercaseName() == headerName.toLowerCase(java.util.Locale.ROOT)).map(_.value())
    if (got.contains(expected)) Right(session) else Left(AuthStrategy.DEF_ERR_REASON)
  }

  override def strip(session: Session): Session =
    session.removeRequestHeader(headerName)
}

object BearerStrategy {
  def fromConfig(cfg: TypesafeConfig)(implicit ec: ExecutionContext): AuthStrategy = {
    val secret = if (cfg.hasPath("secret")) cfg.getString("secret") else ""
    BearerStrategy(secret)
  }
}

final case class HeaderStrategy(secret: String, name: String)(implicit ec: ExecutionContext) extends AuthStrategy {
  override val id: String = "header"
  private val headerName = Option(name).getOrElse("x-api-key").trim match {
    case "" => "x-api-key"
    case n => n
  }

  override def verify(session: Session): Either[String, Session] = {
    val expected = Option(secret).getOrElse("")
    val got = session.requestHeaders.find(_.lowercaseName() == headerName.toLowerCase(java.util.Locale.ROOT)).map(_.value())
    if (got.contains(expected)) Right(session) else Left(AuthStrategy.DEF_ERR_REASON)
  }

  override def strip(session: Session): Session =
    session.removeRequestHeader(headerName)
}

object HeaderStrategy {
  def fromConfig(cfg: TypesafeConfig)(implicit ec: ExecutionContext): AuthStrategy = {
    val secret = if (cfg.hasPath("secret")) cfg.getString("secret") else ""
    val name = if (cfg.hasPath("name")) cfg.getString("name") else "x-api-key"
    HeaderStrategy(secret, name)
  }
}

final case class UnknownStrategy(raw: String)(implicit ec: ExecutionContext) extends AuthStrategy {
  override val id: String = Option(raw).getOrElse("").trim
  override def verify(session: Session): Either[String, Session] = Left("unknown_strategy")
  override def strip(session: Session): Session = session
}

final class AuthProcessor(strategy: AuthStrategy)(implicit ec: ExecutionContext) extends BidirectionalProcessor {

  override val name: String = "Auth"
  private val log = Logger(name)

  override def processRequest(session: Session): Future[Session] =
    strategy.verify(session) match {
      case Right(s0) =>
        Future.successful(strategy.strip(s0))
      case Left(reason) =>
        log.warn(s"Unauthorized request: strategy='${strategy.id}', reason='$reason'")
        Future.successful(
          session.reject(
            code = 401,
            message = "Unauthorized",
            processorName = name,
            details = Some(reason)
          )
        )
    }

  override def processResponse(session: Session): Future[Session] =
    Future.successful(session)

  override def toString: String = s"$name(${strategy.id})"
}

object AuthProcessor extends ProcessorConfigurable {
  override val tpe: String = "auth"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val strategy = AuthStrategy.fromConfig(cfg, actorSystem.settings.config)
    Seq(new AuthProcessor(strategy))
  }
}

