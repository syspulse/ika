package io.syspulse.ika.processor.rpc3

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._
import akka.util.ByteString

import io.syspulse.ika.processor.Session
import io.syspulse.ika.processor.Processor
import io.syspulse.ika.processor.core.CacheProcessor
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

  private val Rpc3MethodKey = "rpc3.method"
  private val Rpc3ParamsKey = "rpc3.params"
  private val Rpc3CacheKeyKey = "rpc3.cacheKey"

  /**
   * Blockchain-specific: Check if this is a request for the current block/slot number.
   * Examples:
   * - EVM: eth_blockNumber
   * - Solana: getSlot
   */
  protected def isBlockNumberRequest(method: String, params: List[Any]): Boolean

  /**
   * Blockchain-specific: Check if this is a block data request (by method name only).
   * Examples:
   * - EVM: eth_getBlockByNumber
   * - Solana: getBlock
   */
  protected def isBlockRequest(method: String): Boolean

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
   * Blockchain-specific: Normalize cache key with actual block identifier from response.
   * Examples:
   * - EVM: Replace "latest" with actual block number like "0x123abc"
   * - Solana: Replace commitment level with actual slot number
   */
  protected def replaceBlockInKey(cacheKey: String, blockIdentifier: String): String

  /**
   * Parse single JSON-RPC request
   */
  private def parseSingleReq(req: String): Option[ProxyRpcReq] = {
    try {
      Some(req.parseJson.convertTo[ProxyRpcReq])
    } catch {
      case e: Exception =>
        val preview = req.take(200).replaceAll("\\s+", " ")
        log.warn(s"Cache: SKIP: failed to parse RPC request: ${e.getMessage}: '$preview'")
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
  def getKey(req: ProxyRpcReq): String = cacheKeyFor(req)

  /** Stable JSON for cache keys (avoids Map/HashMap field-order drift in params.toString). */
  private def paramValueToJson(value: Any): JsValue = value match {
    case n: Int             => JsNumber(n)
    case n: Long            => JsNumber(n)
    case n: BigInt          => JsNumber(n)
    case n: Double          => JsNumber(n)
    case n: BigDecimal      => JsNumber(n)
    case n: java.math.BigDecimal => JsNumber(n)
    case s: String          => JsString(s)
    case true               => JsTrue
    case false              => JsFalse
    case m: Map[_, _] @unchecked =>
      JsObject(
        m.asInstanceOf[Map[String, Any]].toSeq.sortBy(_._1).map { case (k, v) => k -> paramValueToJson(v) }: _*
      )
    case seq: Seq[_]        => JsArray(seq.map(paramValueToJson).toVector)
    case arr: Array[_]      => JsArray(arr.map(paramValueToJson).toVector)
    case other              => JsString(other.toString)
  }

  private def cacheKeyFor(req: ProxyRpcReq): String = {
    val paramsJson = JsArray(req.params.map(paramValueToJson)).compactPrint
    s"${req.method}-$paramsJson"
  }

  import io.syspulse.ika.processor.core.HeaderProcessor
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
    val s0 = enrichSessionWithRpc3Data(session)
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
    val s0 = enrichSessionWithRpc3Data(session)
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
                // All items uncached: return upstream immediately, cache batch items in background.
                runBatchResponseCacheAsync(down, resp)
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
   * Parse RPC3 request metadata into session before cache lookup/store.
   */
  override protected def handleRequestPhase(session: Session): Future[Session] = {
    super.handleRequestPhase(enrichSessionWithRpc3Data(session))
  }

  private def enrichSessionWithRpc3Data(session: Session): Session = {
    if (session.getData[String](Rpc3CacheKeyKey).isDefined) return session

    val reqString = session.requestBody.utf8String.trim
    if (!reqString.startsWith("{")) return session

    parseSingleReq(reqString).fold(session) { req =>
      val cacheKey = cacheKeyFor(req)      
      session
        .putData(Rpc3MethodKey, req.method)
        .putData(Rpc3ParamsKey, req.params)
        .putData(Rpc3CacheKeyKey, cacheKey)
    }
  }

  /**
   * Override: Return cache key parsed once in [[enrichSessionWithRpc3Data]] (single requests only)
   */
  override protected def getCacheKey(session: Session): Option[String] = {
    session.getData[String](Rpc3CacheKeyKey) match {
      case some @ Some(_) => some
      case None =>
        val reqString = session.requestBody.utf8String.trim
        if (reqString.startsWith("[")) {
          // Batch requests use [[handleBatchRequestPhase]] instead of this path.
          None
        } else if (!reqString.startsWith("{")) {
          log.warn(s"Cache: SKIP: request is not JSON-RPC object or batch: '${reqString.take(120)}'")
          None
        } else {
          log.warn(s"Cache: SKIP: could not build cache key for single request: '${reqString.take(120)}'")
          None
        }
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
   * Override: Store response with RPC3-specific logic (block caching)
   */
  override protected def storeInCache(session: Session, cacheKey: String, response: ByteString): Future[Session] = {
    val now = System.currentTimeMillis()
    val method = session.getData[String](Rpc3MethodKey).getOrElse("")
    storeResponseInCache(method, cacheKey, response, now)
    Future.successful(session)
  }

  private def storeResponseInCache(method: String, cacheKey: String, response: ByteString, now: Long): Unit = {
    
    log.info(s"Cache(${cache.size}): STORE: $cacheKey")
    
    cache.put(cacheKey, CacheEntry(now, response))    

    if (isBlockRequest(method)) {
      extractBlockIdentifier(response) match {
        case Some(blockId) if blockId.nonEmpty =>
          val keyBlock = replaceBlockInKey(cacheKey, blockId)
          if (keyBlock != cacheKey) {
            cache.put(keyBlock, CacheEntry(now, response))
            log.info(s"Cache: STORE: $cacheKey: block=$keyBlock")
          } else {
            
          }
        case _ =>
          log.warn(s"Could not extract block")
      }
    }
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

    val keys = requestPairs.map { case (req, _) =>
      val key = getKey(req)
      log.debug(s"Cache: LOOKUP: $key")
      key
    }
    val now = System.currentTimeMillis()

    // Check cache for all items
    val cachedResponses: Seq[Option[ByteString]] = keys.map { key =>
      cache.get(key) match {
        case Some(entry) if now - entry.ts < getTTL(key) =>
          log.debug(s"Cache: HIT: $key")
          Some(entry.response)
        case Some(_) =>
          cache.remove(key)
          log.debug(s"Cache: MISS (expired): $key")
          None
        case None =>
          log.debug(s"Cache: MISS: $key")
          None
      }
    }

    // Pair requests with cached responses
    val allPairs: Seq[((ProxyRpcReq, String), Option[ByteString])] = requestPairs.zip(cachedResponses)
    val uncachedPairs = allPairs.filter(_._2.isEmpty)

    if (uncachedPairs.isEmpty) {
      // All cached - return early
      val response = ByteString(s"[${allPairs.map(_._2.get.utf8String).mkString(",")}]")
      
      log.debug(s"Cache: HIT: batch=${requestPairs.size}")
      log.info(s"Req([${session.requestBody.size}]) <-- Cache([${response.size}],${keys.mkString(",")})")
      
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

      log.debug(s"BATCH(hot=${cachedResponses.flatten.size},cold=${uncachedPairs.size}): request='${session.requestBody.utf8String}'")

      // optimization to avoid modifying when none are uncached
      val modifiedBatchRequest = if(uncachedJsonStrings.size == 0)
        session.requestBody
      else
        ByteString(s"[${uncachedJsonStrings.mkString(",")}]")
      
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
            storeResponseInCache(req.method, key, ByteString(freshResp), now)
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

  private def hasCachedBatchItems(session: Session): Boolean =
    session.getData[Seq[((ProxyRpcReq, String), Option[ByteString])]]("batchAllPairs")
      .exists(_.exists(_._2.isDefined))

  private def runBatchResponseCacheAsync(session: Session, response: ByteString): Unit = {
    val _ = Future(handleBatchResponsePhase(session, response))
      .flatMap(identity)
      .recover { case e =>
        log.warn(s"Cache: async batch write failed: ${e.getMessage}")
        session
      }
  }

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
