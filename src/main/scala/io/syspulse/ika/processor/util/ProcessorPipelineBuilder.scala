package io.syspulse.ika.processor.util

import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem
import com.typesafe.config.{Config => TypesafeConfig}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

import io.syspulse.ika.processor.core._
import io.syspulse.ika.processor.Processor
import io.syspulse.ika.processor.uri.{CacheURI, PoolURI, Rpc3URI}
import io.syspulse.ika.processor.{ProcessorPipeline}
import io.syspulse.ika.processor.rpc3.{Rpc3Processor, SolanaProcessor, EvmProcessor}
import io.syspulse.ika.processor.ai.{AIRouterProcessor, AITokensProcessorConfig}
import io.syspulse.ika.processor.ai.AIRouterProcessor
import io.syspulse.ika.processor.ai.AITokensProcessor

/**
 * ProcessorPipelineBuilder builds pipelines from:
 * - built-in programmatic profiles (web3/http-pool/ai/proxy)
 * - Typesafe config sections (processors=..., profile.<name>.processors=...)
 */
object ProcessorPipelineBuilder {

  def buildWeb3Pipeline(
    destinations: Seq[String],
    profile: PipelineProfile,
    poolUri: String = "sticky://",
    cacheUri: String = "rpc3://"
  )(implicit ec: ExecutionContext, actorSystem: ActorSystem): ProcessorPipeline = {
    implicit val scheduler: akka.actor.Scheduler = actorSystem.scheduler

    val processors = Seq(
      Some(RejectionProcessor.jsonRpc(httpStatusCode = 200)),
      if (profile.throttle > 0) Some(new ThrottleProcessor(profile.throttle)) else None,
      Some(CacheProcessor.fromUri(CacheURI(cacheUri))),
      Some(PoolProcessor.fromUri(PoolURI(poolUri), destinations)),
      Some(Rpc3Processor.fromUri(Rpc3URI(cacheUri))),
      Some(new TimeoutProcessor(profile.timeout, Some(profile.retryDelay))),
      Some(new RetryProcessor(maxRetries = profile.retry, delayMs = profile.retryDelay)),
      Some(new HttpProcessor(compression = profile.compress))
    ).flatten

    ProcessorPipeline.fromSeq(processors, "Web3Pipeline")
  }

  def buildHttpPoolPipeline(
    destinations: Seq[String],
    profile: PipelineProfile,
    poolUri: String = "sticky://"
  )(implicit ec: ExecutionContext, actorSystem: ActorSystem): ProcessorPipeline = {
    val processors = Seq(
      RejectionProcessor.jsonRpc(httpStatusCode = 200),
      new TimeoutProcessor(profile.timeout),
      PoolProcessor.fromUri(PoolURI(poolUri), destinations),
      new HttpProcessor(compression = profile.compress)
    )

    ProcessorPipeline.fromSeq(processors, "HttpPoolPipeline")
  }

  def buildAIPipeline(
    destinations: Seq[String],
    profile: PipelineProfile,
    poolUri: String = "sticky://"
  )(implicit ec: ExecutionContext, actorSystem: ActorSystem): ProcessorPipeline = {
    implicit val scheduler: akka.actor.Scheduler = actorSystem.scheduler

    val processors = Seq(
      Some(RejectionProcessor.restApi(defaultHttpStatus = 500)),
      if (profile.throttle > 0) Some(new ThrottleProcessor(profile.throttle)) else None,
      Some(new TimeoutProcessor(profile.timeout, Some(profile.retryDelay))),
      Some(AIRouterProcessor()),
      Some(PoolProcessor.fromUri(PoolURI(poolUri), destinations)),
      Some(new RetryProcessor(maxRetries = profile.retry, delayMs = profile.retryDelay)),
      Some(AITokensProcessor()),
      Some(new HttpProcessor(compression = profile.compress))
    ).flatten

    ProcessorPipeline.fromSeq(processors, "AIPipeline")
  }
  
  /** Build a pipeline from `profile.<name>.processors` in config. */
  def fromProfile(cfg: TypesafeConfig, profile: String, telemetry: Option[io.syspulse.ika.telemetry.Telemetry])(implicit ec: ExecutionContext, actorSystem: ActorSystem): ProcessorPipeline =
    fromConfig(cfg, profile, telemetry)

  /**
   * Build a pipeline from Typesafe configuration similar to `conf/application-ika.conf`.
   *
   * Expected structure:
   * - processors = "throttle_1, pool_1, http_1"
   * - throttle_1 { throttle = 3000 }
   * - pool_1 { strategy = "lb", destinations = [ "host1=http://...", "host2=http://..." ] }
   * - cache_1 { strategy = "cache://", ttl = 1000, gc = 60000 }
   * - http_1 { method = "GET|POST", headers = { "Content-Type" = "application/json" } }
   *
   * Unknown processor ids are ignored.
   */
  def fromConfig(cfg: TypesafeConfig, telemetry: Option[io.syspulse.ika.telemetry.Telemetry] = None)(implicit ec: ExecutionContext, actorSystem: ActorSystem): ProcessorPipeline = {
    val log = Logger("ProcessorPipelineBuilder.fromConfig")

    val supported: Map[String, ProcessorConfigurable] = Seq[ProcessorConfigurable](
      ThrottleProcessor,
      TimeoutProcessor,
      HeaderProcessor,
      AuthProcessor,
      MetaProcessor,
      RetryProcessorConfig,
      RejectionProcessorConfig,
      CacheProcessor,
      PoolProcessor,
      ProxyProcessor,
      HttpProcessor,
      
      Rpc3Processor,
      //SolanaProcessor,
      //EvmProcessor,

      AIRouterProcessor,
      AITokensProcessorConfig
    ).map(b => b.tpe -> b).toMap

    def has(path: String): Boolean = cfg.hasPath(path)
    def getStringOpt(path: String): Option[String] = if (has(path)) Some(cfg.getString(path)) else None

    val ids: Seq[String] =
      getStringOpt("processors")
        .map(_.split(',').toSeq.map(_.trim).filter(_.nonEmpty))
        .getOrElse(Seq.empty)

    val processors: Seq[Processor] = ids.flatMap { id =>
      if (!cfg.hasPath(id)) {
        log.warn(s"Ignoring processor id without config section: '$id'")
        Nil
      } else {
        val c = cfg.getConfig(id)
        val rawType = if (c.hasPath("type")) c.getString("type") else ""
        val tpe = rawType.split("://", 2).toList match {
          case head :: _ => head.trim
          case _ => rawType.trim
        }
        supported.get(tpe) match {
          case Some(b) =>
            b.fromConfig(id, c)
          case None =>
            log.warn(s"Ignoring unsupported processor type: '$rawType' (id='$id')")
            Nil
        }
      }
    }

    ProcessorPipeline.fromSeq(processors, "ConfigPipeline", telemetry)
  }

  /**
   * Build a pipeline from an application.conf which defines processor sections at root
   * and profile selector blocks under `profile.<name>`.
   *
   * Example:
   * profile {
   *   web3 = { processors = "throttle_1, pool_1, http_1" }
   * }
   */
  def fromConfig(cfg: TypesafeConfig, profile: String, telemetry: Option[io.syspulse.ika.telemetry.Telemetry])(implicit ec: ExecutionContext, actorSystem: ActorSystem): ProcessorPipeline = {
    val log = Logger("ProcessorPipelineBuilder.fromConfig(profile)")
    val path = s"profiles.${profile}"
    if (!cfg.hasPath(path)) {
      log.warn(s"Profiles section not found: '$path' (falling back to root processors)")
      return fromConfig(cfg, telemetry)
    }

    // Use root config for processor definitions, but take processor id list from the profile section.
    val ids: Seq[String] =
      if (cfg.getConfig(path).hasPath("processors"))
        cfg.getConfig(path).getString("processors").split(',').toSeq.map(_.trim).filter(_.nonEmpty)
      else
        Seq.empty

    if (ids.isEmpty) {
      log.warn(s"No processors defined for profile '$profile' (falling back to root processors)")
      return fromConfig(cfg, telemetry)
    }

    // Create a small overlay config that only provides `processors = ...` while keeping root sections accessible.
    val overlay = com.typesafe.config.ConfigFactory.parseString(s"""processors="${ids.mkString(", ")}"""")
    fromConfig(overlay.withFallback(cfg).resolve(), telemetry)
  }
}
