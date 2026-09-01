package io.syspulse.ika.processor.rpc3

import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.Logger
import spray.json._
import akka.util.ByteString

/**
 * EvmProcessor handles Ethereum Virtual Machine (EVM) compatible chains.
 *
 * Supports Ethereum-specific RPC methods:
 * - eth_blockNumber: Returns the current block number
 * - eth_getBlockByNumber: Returns block data by block number or "latest"
 *
 * Caching strategy:
 * - eth_blockNumber responses: cached with short TTL (hot cache)
 * - eth_getBlockByNumber: cached by method + params; "latest" requests also
 *   get a second entry keyed by the resolved block number from the response
 *
 * This allows efficient caching while ensuring latest data freshness.
 */
class EvmProcessor(
  strategy: String = "cache",
  ttl: Long = 30000L,
  ttlLatest: Long = 12000L,
  gcFreq: Long = 10000L,
  skipCaching: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends Rpc3Processor(strategy, ttl, ttlLatest, gcFreq, skipCaching) {

  import ProxyJson._

  private val log = Logger(s"${this.getClass.getSimpleName}")

  override val name: String = "EvmRpc3"

  /**
   * Check if this is an eth_blockNumber request
   */
  override protected def isBlockNumberRequest(method: String, params: List[Any]): Boolean = {
    method == "eth_blockNumber"
  }

  override protected def isBlockRequest(method: String): Boolean = {
    method == "eth_getBlockByNumber"
  }

  /**
   * Extract block number from Ethereum block response
   */
  override protected def extractBlockIdentifier(response: ByteString): Option[String] = {
    try {
      val blockRes = response.utf8String.parseJson.convertTo[ProxyRpcBlockRes]
      val blockNumber = blockRes.result.number

      if (blockNumber != null && blockNumber.nonEmpty) {
        Some(blockNumber)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        log.warn(s"Could not parse EVM block response: ${e.getMessage}")
        None
    }
  }

  override protected def replaceBlockInKey(cacheKey: String, blockIdentifier: String): String = {
    cacheKey.replaceAll("latest", blockIdentifier)
  }

  override protected def getTTL(cacheKey: String): Long = {
    if (cacheKey.startsWith("eth_blockNumber")) {
      ttlLatest
    } else {
      ttl
    }
  }

  override def toString: String = s"${name}($strategy,$ttl,$ttlLatest,$gcFreq)"
}

object EvmProcessor {
  /**
   * Create an EvmProcessor with no caching
   */
  def none()(implicit ec: ExecutionContext): EvmProcessor = {
    new EvmProcessor(strategy = "none")
  }

  /**
   * Create an EvmProcessor with cache strategy.
   */
  def cache(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): EvmProcessor = {
    new EvmProcessor(
      strategy = "cache",
      ttl = ttl,
      ttlLatest = ttlLatest,
      gcFreq = gcFreq,
      skipCaching = skipCaching
    )
  }

  def cacheAsync(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): EvmProcessor = {
    new EvmProcessor(
      strategy = "cache_async",
      ttl = ttl,
      ttlLatest = ttlLatest,
      gcFreq = gcFreq,
      skipCaching = skipCaching
    )
  }

  /** Backward-compatible alias for old callers. */
  def expire(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): EvmProcessor =
    cache(ttl = ttl, ttlLatest = ttlLatest, gcFreq = gcFreq, skipCaching = skipCaching)
}
