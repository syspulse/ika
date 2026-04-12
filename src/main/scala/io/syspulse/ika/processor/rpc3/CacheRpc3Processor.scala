package io.syspulse.ika.processor.rpc3

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._

import io.syspulse.ika.processor.Session
import io.syspulse.ika.processor.impl.CacheProcessor
import io.syspulse.ika.processor.rpc3.{ProxyRpcReq, ProxyRpcBlockRes, ProxyJson}

/**
 * CacheRpc3Processor extends CacheProcessor with RPC3-specific caching logic.
 *
 * RPC3-specific features:
 * - Parses JSON-RPC requests to extract method and params for cache keys
 * - Skips caching for batch requests
 * - Skips caching for error responses (null results, error fields)
 * - Special "latest" block handling:
 *   - eth_blockNumber responses cached with shorter TTL (hot cache)
 *   - eth_getBlockByNumber with "latest" param:
 *     - Response cached as "latest" with short TTL
 *     - Also cached with actual block number for long-term (cold cache)
 * - Generates cache keys from method and params
 *
 * This processor extracts the caching logic that was previously in ProxyCacheExpire.
 */
class CacheRpc3Processor(
  mode: String = "expire",
  ttl: Long = 30000L,              // Default TTL for regular blocks
  ttlLatest: Long = 12000L,        // TTL for "latest" blocks (hot cache)
  gcFreq: Long = 10000L,
  skipCaching: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends CacheProcessor(mode, ttl, gcFreq, skipCaching) {

  import ProxyJson._

  private val log = Logger(s"${this.getClass.getSimpleName}")

  override val name: String = "CacheRpc3"

  /**
   * Parse single JSON-RPC request
   */
  private def parseSingleReq(req: String): Option[ProxyRpcReq] = {
    try {
      Some(req.parseJson.convertTo[ProxyRpcReq])
    } catch {
      case e: Exception =>
        log.debug(s"Failed to parse RPC request: ${e.getMessage}")
        None
    }
  }

  /**
   * Generate RPC3-specific cache key from method and params
   */
  private def getRpc3CacheKey(method: String, params: List[Any]): String = {
    s"$method-${params.toString}"
  }

  /**
   * Override: Generate cache key from RPC3 request
   */
  override protected def getCacheKey(session: Session): Option[String] = {
    // Only cache single JSON-RPC requests (not batch)
    if (!session.requestBody.trim.startsWith("{")) {
      log.debug("Skipping cache for non-single request (batch or invalid)")
      return None
    }

    parseSingleReq(session.requestBody).map { req =>
      val key = getRpc3CacheKey(req.method, req.params)

      // Store method and params in session for response phase
      session.putData("rpc3.method", req.method)
      session.putData("rpc3.params", req.params)

      key
    }
  }

  /**
   * Override: Check if request should be cached
   */
  override protected def shouldCache(session: Session, cacheKey: String): Boolean = {
    // Use parent's skip logic
    super.shouldCache(session, cacheKey)
  }

  /**
   * Override: Check if response should be cached (skip errors)
   */
  override protected def shouldCacheResponse(session: Session, response: String): Boolean = {
    // Skip caching for error responses
    if (isError(response)) {
      log.debug("Skipping cache for error response")
      return false
    }

    true
  }

  /**
   * Check if response contains an error
   */
  private def isError(response: String): Boolean = {
    // Fast and dirty error detection
    (response.contains(""""error"""") && response.contains(""""code"""")) ||
      (response.contains(""""result":null""") || response.contains(""""result": null"""))
  }

  /**
   * Override: Store response with RPC3-specific logic (block number caching)
   */
  override protected def storeInCache(session: Session, cacheKey: String, response: String): Future[Session] = {
    val now = System.currentTimeMillis()

    // Check if this is a "latest" block request
    val isLatestBlockNumber = cacheKey.startsWith(getRpc3CacheKey("eth_blockNumber", List()))
    val isLatestBlock = cacheKey.startsWith(getRpc3CacheKey("eth_getBlockByNumber", List("latest")).stripSuffix(")"))

    // Store in cache with appropriate TTL
    if (isLatestBlockNumber || isLatestBlock) {
      // Hot cache - shorter TTL for "latest" blocks
      cache.put(cacheKey, CacheEntry(now, response))
      log.info(s"Caching 'latest' response (TTL=${ttlLatest}ms): $cacheKey")

      // For eth_getBlockByNumber with "latest", also cache with actual block number
      if (isLatestBlock) {
        try {
          val blockRes = response.parseJson.convertTo[ProxyRpcBlockRes]
          val blockNumber = blockRes.result.number

          if (blockNumber != null && blockNumber.nonEmpty) {
            // Replace 'latest' with actual block number for cold cache
            val keyBlock = cacheKey.replaceAll("latest", blockNumber)
            cache.put(keyBlock, CacheEntry(now, response))
            log.info(s"Caching block number response (TTL=${ttl}ms): $keyBlock")
          }
        } catch {
          case e: Exception =>
            log.warn(s"Could not parse latest block response: ${e.getMessage}")
        }
      }
    } else {
      // Regular cache - normal TTL
      cache.put(cacheKey, CacheEntry(now, response))
      log.info(s"Caching response (TTL=${ttl}ms): $cacheKey")
    }

    Future.successful(session)
  }

  /**
   * Override: Get TTL based on cache key (shorter for "latest" blocks)
   */
  override protected def getTTL(cacheKey: String): Long = {
    val isLatest = cacheKey.contains("latest") ||
      cacheKey.startsWith(getRpc3CacheKey("eth_blockNumber", List()))

    if (isLatest) ttlLatest else ttl
  }

  override def toString: String =
    s"CacheRpc3Processor(mode=$mode, ttl=${ttl}ms, ttlLatest=${ttlLatest}ms, gcFreq=${gcFreq}ms)"
}

object CacheRpc3Processor {
  /**
   * Create a CacheRpc3Processor with no caching
   */
  def none()(implicit ec: ExecutionContext): CacheRpc3Processor = {
    new CacheRpc3Processor(mode = "none")
  }

  /**
   * Create a CacheRpc3Processor with expire mode
   */
  def expire(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): CacheRpc3Processor = {
    new CacheRpc3Processor(
      mode = "expire",
      ttl = ttl,
      ttlLatest = ttlLatest,
      gcFreq = gcFreq,
      skipCaching = skipCaching
    )
  }
}
