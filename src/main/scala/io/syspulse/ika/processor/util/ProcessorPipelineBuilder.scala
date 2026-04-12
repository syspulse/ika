package io.syspulse.ika.processor.util

import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem

import io.syspulse.ika.processor.impl._
import io.syspulse.ika.processor.uri.CacheURI
import io.syspulse.ika.processor.{ProcessorPipeline}
import io.syspulse.ika.processor.rpc3.Rpc3Processor

/**
 * PipelineBuilder constructs processor pipelines from configuration.
 *
 * Supports multiple pipeline profiles:
 * - "web3" - Web3 RPC with caching, throttling, load balancing, retry, timeout
 * - "ai" - AI API with model routing, token extraction
 * - "simple" - Basic proxy without caching or advanced features
 * - "custom" - User-defined processor chain from config
 *
 * Default pipeline for web3:
 * Throttle → Timeout → Cache → LoadBalancer → Retry[HttpClient] → JsonRpcRejection
 */
class ProcessorPipelineBuilder(
  destinations: Seq[String],
  processorConfig: ProcessorConfig = ProcessorConfig.default,
  poolStrategy: String = "sticky",
  cacheUri: String = "rpc3://"  // Default to RPC3 cache for Web3 use case
)(
  implicit ec: ExecutionContext,
  actorSystem: ActorSystem
) {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  implicit val scheduler: akka.actor.Scheduler = actorSystem.scheduler

  /**
   * Build a pipeline based on the configured profile
   */
  def build(profile: String = "web3"): ProcessorPipeline = {
    profile.toLowerCase match {
      case "web3" => buildWeb3Pipeline()
      case "simple" => buildSimplePipeline()
      case "ai" => buildAIPipeline()
      case other =>
        log.warn(s"Unknown pipeline profile: $other, using web3")
        buildWeb3Pipeline()
    }
  }

  /**
   * Build Web3 RPC pipeline with caching, load balancing, retry
   *
   * Pipeline:
   * Throttle → Timeout → Cache → LoadBalancer → Rpc3 → Retry[HttpClient] → JsonRpcRejection
   */
  def buildWeb3Pipeline(): ProcessorPipeline = {
    log.info("Building Web3 RPC pipeline")

    // Build processors
    val processors = Seq(
      // Throttle if configured
      if (processorConfig.throttle > 0) {
        Some(new ThrottleProcessor(processorConfig.throttle, global = true))
      } else None,

      // Set timeout
      Some(new TimeoutProcessor(processorConfig.timeout, Some(processorConfig.retryDelay))),

      // Cache (RPC3-specific with block number handling)
      Some(CacheURI.parse(cacheUri)),

      // Load balancer
      Some(buildLoadBalancer()),

      // RPC3 processor - filters problematic headers for QuickNode
      Some(Rpc3Processor()),

      // Retry wrapping HTTP client
      Some(new RetryProcessor(
        wrapped = HttpClientProcessor(processorConfig.compress),
        maxRetries = processorConfig.retry,
        delayMs = processorConfig.retryDelay
      )),

      // JSON-RPC rejection handler (at the end to handle any rejections)
      Some(RejectionProcessor.jsonRpc(httpStatusCode = 200))
    ).flatten

    ProcessorPipeline.fromSeq(processors, "Web3Pipeline")
  }

  /**
   * Build simple pipeline without caching or retry
   *
   * Pipeline:
   * Timeout → LoadBalancer → HttpClient → JsonRpcRejection
   */
  def buildSimplePipeline(): ProcessorPipeline = {
    log.info("Building Simple pipeline")

    val processors = Seq(
      new TimeoutProcessor(processorConfig.timeout),
      buildLoadBalancer(),
      HttpClientProcessor(processorConfig.compress),
      RejectionProcessor.jsonRpc(httpStatusCode = 200)
    )

    ProcessorPipeline.fromSeq(processors, "SimplePipeline")
  }

  /**
   * Build AI API pipeline with model routing
   *
   * Pipeline:
   * Throttle → Timeout → AIRouter → LoadBalancer → Retry[HttpClient] → AITokens → RestApiRejection
   */
  def buildAIPipeline(): ProcessorPipeline = {
    log.info("Building AI API pipeline")

    val processors = Seq(
      // Throttle if configured
      if (processorConfig.throttle > 0) {
        Some(new ThrottleProcessor(processorConfig.throttle, global = true))
      } else None,

      // Set timeout
      Some(new TimeoutProcessor(processorConfig.timeout, Some(processorConfig.retryDelay))),

      // AI Router - extract model and set pool for load balancing
      Some(AIRouterProcessor()),

      // Load balancer (uses pool from AIRouter)
      Some(buildLoadBalancer()),

      // Retry wrapping HTTP client
      Some(new RetryProcessor(
        wrapped = HttpClientProcessor(processorConfig.compress),
        maxRetries = processorConfig.retry,
        delayMs = processorConfig.retryDelay
      )),

      // AI Tokens - extract token usage from response
      Some(AITokensProcessor()),

      // REST API rejection handler (AI APIs typically use REST format)
      Some(RejectionProcessor.restApi(defaultHttpStatus = 500))
    ).flatten

    ProcessorPipeline.fromSeq(processors, "AIPipeline")
  }

  /**
   * Build load balancer processor
   */
  private def buildLoadBalancer(): LoadBalancerProcessor = {
    log.info(s"Creating LoadBalancer with strategy: $poolStrategy, destinations: ${destinations.size}")

    poolStrategy.toLowerCase match {
      case "sticky" => LoadBalancerProcessor.sticky(destinations)
      case "roundrobin" | "lb" => LoadBalancerProcessor.roundRobin(destinations)
      case "random" => LoadBalancerProcessor.random(destinations)
      case "hash" => LoadBalancerProcessor.hashSticky(destinations)
      case _ => LoadBalancerProcessor.sticky(destinations)
    }
  }
}

object ProcessorPipelineBuilder {
  def apply(
    destinations: Seq[String],
    processorConfig: ProcessorConfig = ProcessorConfig.default,
    poolStrategy: String = "sticky",
    cacheUri: String = "rpc3://"
  )(implicit ec: ExecutionContext, actorSystem: ActorSystem): ProcessorPipelineBuilder = {
    new ProcessorPipelineBuilder(destinations, processorConfig, poolStrategy, cacheUri)(ec, actorSystem)
  }
}
