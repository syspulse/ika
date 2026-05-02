package io.syspulse.ika.processor.rpc3

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._
import akka.util.ByteString

import io.syspulse.ika.processor.Session
import io.syspulse.ika.processor.Processor
import io.syspulse.ika.processor.impl.CacheProcessor
import io.syspulse.ika.processor.rpc3.{ProxyRpcReq, ProxyRpcBlockRes, ProxyJson}
import io.syspulse.ika.processor.uri.Rpc3URI
import io.syspulse.ika.processor.util.ProcessorConfigurable
import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem
import io.syspulse.ika.processor.ResponseSource

/**
 * Rpc3Processor is the abstract base class for blockchain-specific RPC caching.
 *
 * Common RPC3 features:
 * - Parses JSON-RPC requests to extract method and params for cache keys
 * - Handles batch requests: checks cache for each item individually
 * - Skips caching for error responses (null results, error fields)
 * - Special "latest" block handling (blockchain-specific via abstract methods)
 * - Generates cache keys from method and params
 *
 * Batch request handling:
 * - Parses batch as array of individual requests
 * - Checks cache for each request individually
 * - If all cached, returns early with assembled batch response
 * - If some uncached, only sends uncached items to downstream
 * - Assembles final response preserving request order
 * - Caches fresh responses individually
 *
 * Blockchain-specific subclasses:
 * - EvmProcessor: Ethereum (eth_blockNumber, eth_getBlockByNumber)
 * - SolanaProcessor: Solana (getSlot, getBlock)
 */
abstract class Rpc3Processor(
  strategy: String = "cache",
  ttl: Long = 30000L,              // Default TTL for regular blocks
  ttlLatest: Long = 12000L,        // TTL for "latest" blocks (hot cache)
  gcFreq: Long = 10000L,
  skipCaching: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends CacheProcessor(strategy, ttl, gcFreq, skipCaching) {

  import ProxyJson._

  private val log = Logger(s"${this.getClass.getSimpleName}")

  override val name: String = "Rpc3"

  /**
   * Blockchain-specific: Check if this is a request for the current block/slot number.
   * Examples:
   * - EVM: eth_blockNumber
   * - Solana: getSlot
   */
  protected def isBlockNumberRequest(method: String, params: List[Any]): Boolean

  /**
   * Blockchain-specific: Check if this is a request for a block by identifier with "latest" parameter.
   * Examples:
   * - EVM: eth_getBlockByNumber with params containing "latest"
   * - Solana: getBlock with params containing commitment level indicating latest
   */
  protected def isLatestBlockRequest(method: String, params: List[Any]): Boolean

  /**
   * Blockchain-specific: Extract the block/slot identifier from a response.
   * Examples:
   * - EVM: Extract "number" field from block response (e.g., "0x123abc")
   * - Solana: Extract "slot" field from block response
   *
   * Returns None if the identifier cannot be extracted.
   */
  protected def extractBlockIdentifier(response: ByteString): Option[String]

  /**
   * Blockchain-specific: Replace "latest" placeholder in cache key with actual block identifier.
   * Examples:
   * - EVM: Replace "latest" with actual block number like "0x123abc"
   * - Solana: Replace commitment level with actual slot number
   */
  protected def replaceLatestInKey(cacheKey: String, blockIdentifier: String): String

  /**
   * Parse single JSON-RPC request
   */
  private def parseSingleReq(req: String): Option[ProxyRpcReq] = {
    try {
      Some(req.parseJson.convertTo[ProxyRpcReq])
    } catch {
      case e: Exception =>
        log.debug(s"Failed to parse RPC request: ${e.getMessage}: '${req}'")
        None
    }
  }

  /**
   * Parse batch JSON-RPC request (array of requests)
   * Returns sequence of (parsed request, original JSON string) pairs
   */
  def decodeBatch(req: String): Seq[(ProxyRpcReq, String)] = {
    try {
      val json = req.parseJson
      json match {
        case JsArray(elements) =>
          elements.flatMap { elem =>
            try {
              val parsed = elem.convertTo[ProxyRpcReq]
              Some((parsed, elem.compactPrint))
            } catch {
              case e: Exception =>
                log.warn(s"Failed to parse batch item: ${e.getMessage}")
                None
            }
          }
        case _ =>
          log.warn(s"Failed to parse batch request: not JSON array")
          Seq.empty
      }
    } catch {
      case e: Exception =>
        log.warn(s"Failed to parse batch request: ${e.getMessage}")
        Seq.empty
    }
  }

  /**
   * Detect if request is a batch (starts with "[")
   */
  private def isBatchRequest(requestBody: ByteString): Boolean = {
    requestBody.utf8String.trim.startsWith("[")
  }

  /**
   * Get cache key for a single RPC request
   */
  def getKey(req: ProxyRpcReq): String = {
    getRpc3CacheKey(req.method, req.params)
  }

  /**
   * Generate RPC3-specific cache key from method and params
   */
  private def getRpc3CacheKey(method: String, params: List[Any]): String = {
    s"$method-${params.toString}"
  }

  import io.syspulse.ika.processor.impl.HeaderProcessor
  private val header = new HeaderProcessor(
    removeRequest = Set("timeout-access", "host"),
    addRequest = Map("Content-Type" -> "application/json")
  )

  /**
   * Normalize RPC request headers in all strategies (including passthrough/none).
   *
   * Some upstream RPC providers validate Host/Content-Type strictly; for rpc3 pipelines
   * we want consistent, cache-key-stable request headers regardless of caching strategy.
   */
  override def process(session: Session): Future[Session] =
    header.processRequest(session).flatMap(super.process)

  /**
   * Override: Process with cache strategy - handle batch vs single requests
   */
  override protected def processCacheStrategy(session: Session): Future[Session] = {
    val s0 = session
    if (isBatchRequest(s0.requestBody)) {
        // Batch request - check cache, call downstream for uncached, then assemble + cache
        handleBatchRequestPhase(s0).flatMap { s =>
          if (s.responseBody.isDefined || s.shouldReturn || s.isRejected) Future.successful(s)
          else {
            next(s).flatMap { down =>
              down.responseBody match {
                case Some(resp) => handleBatchResponsePhase(down, resp)
                case None       => Future.successful(down)
              }
            }
          }
        }
    } else {
      // Single request - use parent's logic
      super.processCacheStrategy(s0)
    }
  }

  override protected def processCacheAsyncStrategy(session: Session): Future[Session] = {
    val s0 = session
    if (isBatchRequest(s0.requestBody)) {
      handleBatchRequestPhase(s0).flatMap { s =>
        if (s.responseBody.isDefined || s.shouldReturn || s.isRejected) Future.successful(s)
        else {
          next(s).flatMap { down =>
            down.responseBody match {
              case Some(resp) if hasCachedBatchItems(down) =>
                // Partial batch cache hits require assembly before returning.
                handleBatchResponsePhase(down, resp)
              case Some(resp) =>
                // All items were uncached: return the upstream batch immediately and cache in background.
                runResponseCacheAsync(down, resp)
                Future.successful(down)
              case None =>
                Future.successful(down)
            }
          }
        }
      }
    } else {
      super.processCacheAsyncStrategy(s0)
    }
  }

  /**
   * Override: Generate cache key from RPC3 request (single requests only)
   */
  override protected def getCacheKey(session: Session): Option[String] = {
    val reqString = session.requestBody.utf8String
    // Only cache single JSON-RPC requests (not batch)
    if (!reqString.trim.startsWith("{")) {
      log.warn("Cache: SKIP: non-single request (batch or invalid)")
      return None
    }

    parseSingleReq(reqString).map { req =>
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
  override protected def shouldCacheResponse(session: Session, response: ByteString): Boolean = {
    // Skip caching for error responses
    if (isError(response)) {
      log.debug(s"Cache: SKIP: error response: '${response.utf8String}'")
      return false
    }

    true
  }

  /**
   * Check if response contains an error
   */
  private def isError(response: ByteString): Boolean = {
    val respString = response.utf8String
    // Fast and dirty error detection
    (respString.contains(""""error"""") && respString.contains(""""code"""")) ||
      (respString.contains(""""result":null""") || respString.contains(""""result": null"""))
  }

  /**
   * Override: Store response with RPC3-specific logic (block number caching)
   */
  override protected def storeInCache(session: Session, cacheKey: String, response: ByteString): Future[Session] = {
    val now = System.currentTimeMillis()

    // Extract method and params from session
    val method = session.getData[String]("rpc3.method").getOrElse("")
    val params = session.getData[List[Any]]("rpc3.params").getOrElse(List.empty)

    // Check if this is a "latest" block request
    val isCurrentBlockNumber = isBlockNumberRequest(method, params)
    val isLatest = isLatestBlockRequest(method, params)

    // Store in cache with appropriate TTL
    if (isCurrentBlockNumber || isLatest) {
      // Hot cache - shorter TTL for "latest" blocks
      cache.put(cacheKey, CacheEntry(now, response))
      log.debug(s"Cache: STORE: 'latest': $cacheKey")

      // For latest block requests, also cache with actual block identifier
      if (isLatest) {
        extractBlockIdentifier(response) match {
          case Some(blockId) if blockId.nonEmpty =>
            val keyBlock = replaceLatestInKey(cacheKey, blockId)
            cache.put(keyBlock, CacheEntry(now, response))
            log.debug(s"Cache: STORE: block: $keyBlock")
          case _ =>
            log.debug(s"Could not extract block identifier from latest block response")
        }
      }
    } else {
      // Regular cache - normal TTL
      cache.put(cacheKey, CacheEntry(now, response))
      log.debug(s"Cache: STORE: $cacheKey")
    }

    Future.successful(session)
  }

  /**
   * Override: Get TTL based on cache key (shorter for "latest" blocks)
   *
   * Note: This is a heuristic check based on cache key content.
   * It checks if the key contains "latest" or matches known block number methods.
   */
  protected def getTTL(cacheKey: String): Long

  // ===== Batch Request Handling =====

  /**
   * Handle batch request phase - check cache for each item
   */
  private def handleBatchRequestPhase(session: Session): Future[Session] = {
    val requestPairs = decodeBatch(session.requestBody.utf8String)

    if (requestPairs.isEmpty) {
      // Empty batch - return early with empty array
      log.debug("Empty batch request")
      return Future.successful(session
        .withResponse(ByteString("[]"), ResponseSource.CACHE)
        .returnEarly("cache_hit")
      )
    }

    val keys = requestPairs.map { case (req, _) => getKey(req) }
    val now = System.currentTimeMillis()

    // Check cache for all items
    val cachedResponses: Seq[Option[ByteString]] = keys.map { key =>
      cache.get(key).flatMap { entry =>
        val entryTTL = getTTL(key)
        if (now - entry.ts < entryTTL) {
          Some(entry.response)
        } else {
          cache.remove(key)
          None
        }
      }
    }

    // Pair requests with cached responses
    val allPairs: Seq[((ProxyRpcReq, String), Option[ByteString])] = requestPairs.zip(cachedResponses)
    val uncachedPairs = allPairs.filter(_._2.isEmpty)

    if (uncachedPairs.isEmpty) {
      // All cached - return early
      val response = ByteString(s"[${allPairs.map(_._2.get.utf8String).mkString(",")}]")
      log.debug(s"Cache: HIT: batch=${requestPairs.size}")
      recordCacheHit(session)

      Future.successful(session
        .withResponse(response, ResponseSource.CACHE)
        .putData("batchCached", true)
        .putData("batchAllPairs", allPairs)
        .returnEarly("cache_hit")
      )
    } else {
      // Some uncached - modify request to only include uncached items using original JSON strings
      val uncachedJsonStrings = uncachedPairs.map(_._1._2) // Get the original JSON string

      val modifiedBatchRequest = ByteString(s"[${uncachedJsonStrings.mkString(",")}]")

      log.debug(s"Cache: STORE: (${cachedResponses.flatten.size} cached, ${uncachedPairs.size} uncached)")
      recordCacheMiss(session)

      Future.successful(session
        .withRequestBody(modifiedBatchRequest)
        .putData("batchAllPairs", allPairs)
        .putData("batchUncachedPairs", uncachedPairs)
        .putData("batchCached", false)
      )
    }
  }

  /**
   * Handle batch response phase - cache fresh responses and assemble final response
   */
  private def handleBatchResponsePhase(session: Session, response: ByteString): Future[Session] = {
    // Check if already fully cached
    session.getData[Boolean]("batchCached") match {
      case Some(true) =>
        log.debug("Cache: HIT")
        return Future.successful(session)
      case _ => // Continue
    }

    // Get the original request/response pairs
    session.getData[Seq[((ProxyRpcReq, String), Option[ByteString])]]("batchAllPairs") match {
      case None =>
        log.debug("Cache: MISS: No batch metadata found, skipping batch assembly")
        return Future.successful(session)

      case Some(allPairs) =>
        // Parse fresh responses
        val freshResponses = parseBatchResponse(response) match {
          case scala.util.Success(responses) =>
            responses
          case scala.util.Failure(e) =>
            log.error(s"Failed to parse batch response: ${e.getMessage}")
            return Future.successful(session.reject(
              code = -32603,
              message = s"Failed to parse batch response: ${e.getMessage}",
              processorName = name
            ))
        }

        // Get uncached pairs
        val uncachedPairs = session.getData[Seq[((ProxyRpcReq, String), Option[ByteString])]]("batchUncachedPairs")
          .getOrElse(Seq.empty)

        // Validate response count matches uncached count
        if (freshResponses.size != uncachedPairs.size) {
          val msg = s"Cache: Batch response mismatch: response size=${freshResponses.size}, expected=${uncachedPairs.size}"
          log.error(msg)
          return Future.successful(session.reject(
            code = -32603,
            message = msg,
            processorName = name
          ))
        }

        // Cache fresh responses
        val now = System.currentTimeMillis()
        freshResponses.zip(uncachedPairs).foreach { case (freshResp, ((req, _), _)) =>
          if (!isError(ByteString(freshResp))) {
            val key = getKey(req)
            storeSingleInCache(key, ByteString(freshResp), now)
          } else {
            log.debug(s"Cache: SKIP: error response: ${req.method}")
          }
        }

        // Assemble final response from cached + fresh (preserving order)
        var freshIdx = 0
        val assembled = allPairs.map { case ((req, jsonStr), cachedOpt) =>
          cachedOpt.map(_.utf8String).getOrElse {
            val fresh = freshResponses(freshIdx)
            freshIdx += 1
            fresh
          }
        }

        val finalResponse = ByteString(s"[${assembled.mkString(",")}]")
        log.debug(s"Cache: HIT: Assembled batch: ${assembled.size}")

        // Preserve the responseSource from the HTTP call (should be REMOTE for fresh data)
        Future.successful(session.withResponseBody(finalResponse))
    }
  }

  /**
   * Parse batch response (array of JSON-RPC responses)
   */
  private def parseBatchResponse(response: ByteString): scala.util.Try[Vector[String]] = {
    scala.util.Try {
      val json = response.utf8String.parseJson
      json match {
        case JsArray(elements) =>
          elements.map(_.compactPrint)
        case _ =>
          throw new Exception("Batch response is not a JSON array")
      }
    }
  }

  /**
   * Store a single response in cache (used by batch caching)
   *
   * Note: For batch requests, we don't have session context with method/params,
   * so we use heuristic checks on the cache key to determine if it's a latest block request.
   */
  private def storeSingleInCache(cacheKey: String, response: ByteString, now: Long): Unit = {
    // Heuristic: check if key contains "latest" or similar indicators
    val isLatestLike = cacheKey.contains("latest") ||
                       cacheKey.contains("finalized") ||
                       cacheKey.contains("confirmed")

    if (isLatestLike) {
      // Hot cache - shorter TTL for "latest" blocks
      cache.put(cacheKey, CacheEntry(now, response))
      log.debug(s"Cache: 'latest': $cacheKey")

      // Try to extract block identifier and create a cold cache entry
      extractBlockIdentifier(response) match {
        case Some(blockId) if blockId.nonEmpty =>
          val keyBlock = replaceLatestInKey(cacheKey, blockId)
          cache.put(keyBlock, CacheEntry(now, response))
          log.debug(s"Cache: block: $keyBlock")
        case _ =>
          log.debug(s"Could not extract block identifier from response")
      }
    } else {
      // Regular cache - normal TTL
      cache.put(cacheKey, CacheEntry(now, response))
      log.debug(s"Cache: STORE: $cacheKey")
    }
  }

  private def hasCachedBatchItems(session: Session): Boolean =
    session.getData[Seq[((ProxyRpcReq, String), Option[ByteString])]]("batchAllPairs")
      .exists(_.exists(_._2.isDefined))

  override def toString: String = s"${name}($strategy,${ttl},${ttlLatest},${gcFreq})"
}

object Rpc3Processor extends ProcessorConfigurable {
  override val tpe: String = "rpc3"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    // Keep config shape similar to cache:// processor, but add rpc3-specific `latest` TTL.
    val rawStrategy = if (cfg.hasPath("strategy")) cfg.getString("strategy") else "cache"
    val strategy = rawStrategy.toLowerCase match {
      case "none" => "none"
      case "expire" | "rpc3" => "cache"
      case "rpc3_async" => "cache_async"
      case other => other
    }

    // Get blockchain type (evm, solana) - defaults to evm for backward compatibility
    val chain = if (cfg.hasPath("chain")) cfg.getString("chain")
               else if (cfg.hasPath("type")) cfg.getString("type")
               else "evm"

    val ttl = if (cfg.hasPath("ttl")) Some(cfg.getLong("ttl")) else None
    val latest =
      if (cfg.hasPath("latest")) Some(cfg.getLong("latest"))
      else if (cfg.hasPath("ttlLatest")) Some(cfg.getLong("ttlLatest"))
      else None

    val gc =
      if (cfg.hasPath("gc")) Some(cfg.getLong("gc"))
      else if (cfg.hasPath("gcFreq")) Some(cfg.getLong("gcFreq"))
      else None

    val q = Seq(
      Some(s"chain=$chain"),
      ttl.map(v => s"ttl=$v"),
      latest.map(v => s"latest=$v"),
      gc.map(v => s"gc=$v")
    ).flatten

    val base = if (strategy.contains("://")) strategy else s"${strategy}://"
    val uri = if (q.nonEmpty) s"${base}?${q.mkString("&")}" else base

    Seq(fromUri(Rpc3URI(uri)))
  }

  /**
   * Create an EVM processor with no caching (backward compatibility)
   */
  def none()(implicit ec: ExecutionContext): Rpc3Processor = {
    EvmProcessor.none()
  }

  /**
   * Create an EVM processor with cache strategy.
   */
  def cache(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): Rpc3Processor = {
    EvmProcessor.cache(ttl, ttlLatest, gcFreq, skipCaching)
  }

  def cacheAsync(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): Rpc3Processor =
    EvmProcessor.cacheAsync(ttl, ttlLatest, gcFreq, skipCaching)

  /** Backward-compatible alias for old callers. */
  def expire(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): Rpc3Processor =
    cache(ttl = ttl, ttlLatest = ttlLatest, gcFreq = gcFreq, skipCaching = skipCaching)

  /**
   * Create a blockchain-specific processor from URI
   */
  def fromUri(c: Rpc3URI, skipCaching: Set[String] = Set.empty)(implicit ec: ExecutionContext): Rpc3Processor = {
    // Get blockchain type from URI ops, default to evm for backward compatibility
    val chain = c.ops.getOrElse("chain", c.ops.getOrElse("type", "evm")).toLowerCase

    chain match {
      case "solana" | "sol" =>
        c.kind match {
          case "none" => SolanaProcessor.none()
          case "cache" => SolanaProcessor.cache(ttl = c.ttl, ttlLatest = c.ttlLatest, gcFreq = c.gcFreq, skipCaching = skipCaching)
          case "cache_async" => SolanaProcessor.cacheAsync(ttl = c.ttl, ttlLatest = c.ttlLatest, gcFreq = c.gcFreq, skipCaching = skipCaching)
          case _      => SolanaProcessor.cache(skipCaching = skipCaching)
        }

      case "evm" | "eth" | "ethereum" | _ =>
        // Default to EVM for backward compatibility
        c.kind match {
          case "none" => EvmProcessor.none()
          case "cache" => EvmProcessor.cache(ttl = c.ttl, ttlLatest = c.ttlLatest, gcFreq = c.gcFreq, skipCaching = skipCaching)
          case "cache_async" => EvmProcessor.cacheAsync(ttl = c.ttl, ttlLatest = c.ttlLatest, gcFreq = c.gcFreq, skipCaching = skipCaching)
          case _      => EvmProcessor.cache(skipCaching = skipCaching)
        }
    }
  }
}
