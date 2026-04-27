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
 * Rpc3Processor extends CacheProcessor with RPC3-specific caching logic.
 *
 * RPC3-specific features:
 * - Parses JSON-RPC requests to extract method and params for cache keys
 * - Handles batch requests: checks cache for each item individually
 * - Skips caching for error responses (null results, error fields)
 * - Special "latest" block handling:
 *   - eth_blockNumber responses cached with shorter TTL (hot cache)
 *   - eth_getBlockByNumber with "latest" param:
 *     - Response cached as "latest" with short TTL
 *     - Also cached with actual block number for long-term (cold cache)
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
 * This processor extracts the caching logic that was previously in ProxyCacheExpire.
 */
class Rpc3Processor(
  mode: String = "expire",
  ttl: Long = 30000L,              // Default TTL for regular blocks
  ttlLatest: Long = 12000L,        // TTL for "latest" blocks (hot cache)
  gcFreq: Long = 10000L,
  skipCaching: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends CacheProcessor(mode, ttl, gcFreq, skipCaching) {

  import ProxyJson._

  private val log = Logger(s"${this.getClass.getSimpleName}")

  override val name: String = "Rpc3"

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
   */
  def decodeBatch(req: String): Seq[ProxyRpcReq] = {
    try {
      val json = req.parseJson
      json match {
        case JsArray(elements) =>
          elements.flatMap { elem =>
            try {
              Some(elem.convertTo[ProxyRpcReq])
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
   * Override: Process with expire mode - handle batch vs single requests
   */
  override protected def processExpireMode(session: Session): Future[Session] = {
    // Normalize request headers first so caching/downstream see the same request.
    header.processRequest(session).flatMap { s0 =>
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
        super.processExpireMode(s0)
      }
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

    // Check if this is a "latest" block request
    val isLatestBlockNumber = cacheKey.startsWith(getRpc3CacheKey("eth_blockNumber", List()))
    val isLatestBlock = cacheKey.startsWith(getRpc3CacheKey("eth_getBlockByNumber", List("latest")).stripSuffix(")"))

    // Store in cache with appropriate TTL
    if (isLatestBlockNumber || isLatestBlock) {
      // Hot cache - shorter TTL for "latest" blocks
      cache.put(cacheKey, CacheEntry(now, response))
      log.debug(s"Cache: STORE: 'latest': $cacheKey")

      // For eth_getBlockByNumber with "latest", also cache with actual block number
      if (isLatestBlock) {
        try {
          val blockRes = response.utf8String.parseJson.convertTo[ProxyRpcBlockRes]
          val blockNumber = blockRes.result.number

          if (blockNumber != null && blockNumber.nonEmpty) {
            // Replace 'latest' with actual block number for cold cache
            val keyBlock = cacheKey.replaceAll("latest", blockNumber)
            cache.put(keyBlock, CacheEntry(now, response))
            log.debug(s"Cache: STORE: block: $keyBlock")
          }
        } catch {
          case e: Exception =>
            log.warn(s"Could not parse latest block response: ${e.getMessage}")
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
   */
  override protected def getTTL(cacheKey: String): Long = {
    val isLatest = cacheKey.contains("latest") ||
      cacheKey.startsWith(getRpc3CacheKey("eth_blockNumber", List()))

    if (isLatest) ttlLatest else ttl
  }

  // ===== Batch Request Handling =====

  /**
   * Handle batch request phase - check cache for each item
   */
  private def handleBatchRequestPhase(session: Session): Future[Session] = {
    val requests = decodeBatch(session.requestBody.utf8String)

    if (requests.isEmpty) {
      // Empty batch - return early with empty array
      log.debug("Empty batch request")
      return Future.successful(session
        .withResponse(ByteString("[]"), ResponseSource.CACHE)
        .returnEarly("cache_hit")
      )
    }

    val keys = requests.map(getKey)
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
    val allPairs: Seq[(ProxyRpcReq, Option[ByteString])] = requests.zip(cachedResponses)
    val uncachedPairs = allPairs.filter(_._2.isEmpty)

    if (uncachedPairs.isEmpty) {
      // All cached - return early
      val response = ByteString(s"[${allPairs.map(_._2.get.utf8String).mkString(",")}]")
      log.debug(s"Cache: HIT: batch=${requests.size}")
      recordCacheHit(session)

      Future.successful(session
        .withResponse(response, ResponseSource.CACHE)
        .putData("batchCached", true)
        .putData("batchAllPairs", allPairs)
        .returnEarly("cache_hit")
      )
    } else {
      // Some uncached - modify request to only include uncached items
      val uncachedRequests = uncachedPairs.map(_._1)
      val modifiedBatchRequest = ByteString(s"[${uncachedRequests.map(_.toJson.compactPrint).mkString(",")}]")

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
    session.getData[Seq[(ProxyRpcReq, Option[ByteString])]]("batchAllPairs") match {
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
        val uncachedPairs = session.getData[Seq[(ProxyRpcReq, Option[ByteString])]]("batchUncachedPairs")
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
        freshResponses.zip(uncachedPairs).foreach { case (freshResp, (req, _)) =>
          if (!isError(ByteString(freshResp))) {
            val key = getKey(req)
            storeSingleInCache(key, ByteString(freshResp), now)
          } else {
            log.debug(s"Cache: SKIP: error response: ${req.method}")
          }
        }

        // Assemble final response from cached + fresh (preserving order)
        var freshIdx = 0
        val assembled = allPairs.map { case (req, cachedOpt) =>
          cachedOpt.map(_.utf8String).getOrElse {
            val fresh = freshResponses(freshIdx)
            freshIdx += 1
            fresh
          }
        }

        val finalResponse = ByteString(s"[${assembled.mkString(",")}]")
        log.debug(s"Cache: HIT: Assembled batch: ${assembled.size}")

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
   */
  private def storeSingleInCache(cacheKey: String, response: ByteString, now: Long): Unit = {
    // Check if this is a "latest" block request
    val isLatestBlockNumber = cacheKey.startsWith(getRpc3CacheKey("eth_blockNumber", List()))
    val isLatestBlock = cacheKey.startsWith(getRpc3CacheKey("eth_getBlockByNumber", List("latest")).stripSuffix(")"))

    // Store in cache with appropriate TTL
    if (isLatestBlockNumber || isLatestBlock) {
      // Hot cache - shorter TTL for "latest" blocks
      cache.put(cacheKey, CacheEntry(now, response))
      log.debug(s"Cache: 'latest': $cacheKey")

      // For eth_getBlockByNumber with "latest", also cache with actual block number
      if (isLatestBlock) {
        try {
          val blockRes = response.utf8String.parseJson.convertTo[ProxyRpcBlockRes]
          val blockNumber = blockRes.result.number

          if (blockNumber != null && blockNumber.nonEmpty) {
            // Replace 'latest' with actual block number for cold cache
            val keyBlock = cacheKey.replaceAll("latest", blockNumber)
            cache.put(keyBlock, CacheEntry(now, response))
            log.debug(s"Cache: block: $keyBlock")
          }
        } catch {
          case e: Exception =>
            log.warn(s"Could not parse latest block response: ${e.getMessage}")
        }
      }
    } else {
      // Regular cache - normal TTL
      cache.put(cacheKey, CacheEntry(now, response))
      log.debug(s"Cache: STORE: $cacheKey")
    }
  }

  override def toString: String = s"${name}($mode,${ttl},${ttlLatest},${gcFreq})"
}

object Rpc3Processor extends ProcessorConfigurable {
  override val tpe: String = "rpc3"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    // Keep config shape similar to cache:// processor, but add rpc3-specific `latest` TTL.
    val rawStrategy = if (cfg.hasPath("strategy")) cfg.getString("strategy") else "rpc3"
    val strategy = rawStrategy.toLowerCase match {
      case "none" => "none"
      // historically people used "expire" here; for rpc3:// it means "use rpc3 caching"
      case "expire" => "rpc3"
      case other => other
    }

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
      ttl.map(v => s"ttl=$v"),
      latest.map(v => s"latest=$v"),
      gc.map(v => s"gc=$v")
    ).flatten

    val base = if (strategy.contains("://")) strategy else s"${strategy}://"
    val uri = if (q.nonEmpty) s"${base}?${q.mkString("&")}" else base

    Seq(fromUri(Rpc3URI(uri)))
  }

  /**
   * Create a Rpc3Processor with no caching
   */
  def none()(implicit ec: ExecutionContext): Rpc3Processor = {
    new Rpc3Processor(mode = "none")
  }

  /**
   * Create a Rpc3Processor with expire mode
   */
  def expire(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): Rpc3Processor = {
    new Rpc3Processor(
      mode = "expire",
      ttl = ttl,
      ttlLatest = ttlLatest,
      gcFreq = gcFreq,
      skipCaching = skipCaching
    )
  }

  
  def fromUri(c: Rpc3URI, skipCaching: Set[String] = Set.empty)(implicit ec: ExecutionContext): Rpc3Processor = {
    c.kind match {
      case "none" =>
        none()
      case "rpc3" =>
        expire(ttl = c.ttl, ttlLatest = c.ttlLatest, gcFreq = c.gcFreq, skipCaching = skipCaching)
      case _ =>
        expire()
    }
  }
}
