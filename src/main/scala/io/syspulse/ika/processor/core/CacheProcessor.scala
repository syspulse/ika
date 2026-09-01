package io.syspulse.ika.processor.core

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.concurrent
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap
import com.typesafe.scalalogging.Logger

import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem
import akka.util.ByteString

import io.syspulse.ika.processor.{Processor, Session}
import io.syspulse.ika.processor.uri.CacheURI
import io.syspulse.ika.processor.ResponseSource
import io.syspulse.ika.telemetry.Telemetry
import io.syspulse.skel.cron.CronFreq
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * CacheProcessor provides generic request/response caching with TTL and garbage collection.
 *
 * This is a base processor that provides caching infrastructure. Subclasses can override
 * getCacheKey() and shouldCache() for protocol-specific caching logic.
 *
 * Strategies:
 * - "none" - No caching (passthrough)
 * - "cache" - Time-based expiration with configurable TTL
 * - "cache_async" - Same cache lookups as "cache", but response cache writes happen asynchronously
 *
 * Features:
 * - Thread-safe concurrent cache
 * - Automatic garbage collection
 * - Cache hit/miss telemetry
 * - Configurable TTL per cache entry
 * - Skip caching for certain request patterns
 *
 * Session processorData:
 * - Writes: "cacheHit" (Boolean) - whether response came from cache
 * - Writes: "fromCache" (Boolean) - same as cacheHit
 * - Writes: "cacheKey" (String) - the cache key used
 */
class CacheProcessor(
  strategy: String = "cache",
  ttl: Long = 30000L,
  gcFreq: Long = 10000L,
  skipCaching: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends Processor {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  override val name: String = "Cache"

  // Thread-safe cache storage
  protected case class CacheEntry(ts: Long, response: ByteString)
  protected val cache: concurrent.Map[String, CacheEntry] = new ConcurrentHashMap[String, CacheEntry]().asScala

  private val effectiveStrategy: String = CacheProcessor.normalizeStrategy(strategy)

  // Garbage collection cron job (only for cache strategies)
  private val cron: Option[CronFreq] = if (effectiveStrategy == "cache" || effectiveStrategy == "cache_async") {
    Some(new CronFreq(
      (_: Long) => {
        val now = System.currentTimeMillis()
        var removed = 0

        cache.foreach { case (key, entry) =>
          if (now - entry.ts >= getTTL(key)) {
            cache.remove(key)
            removed += 1
          }
        }

        val size = cache.size
        log.debug(s"GC: size=$size, removed=$removed")
        true
      },
      s"$gcFreq",
      gcFreq
    ))
  } else {
    None
  }

  // Start GC if caching is enabled
  cron.foreach(_.start())

  /**
   * Process - handles both request (cache lookup) and response (cache store)
   */
  override def process(session: Session): Future[Session] = {
    effectiveStrategy match {
      case "none" =>
        // No caching - pass through
        next(session)

      case "cache" =>
        processCacheStrategy(session)

      case "cache_async" =>
        processCacheAsyncStrategy(session)

      case _ =>
        log.warn(s"Unknown cache strategy: '$strategy' (using passthrough)")
        next(session)
    }
  }

  /**
   * Process with cache strategy - check cache on request, store on response.
   */
  protected def processCacheStrategy(session: Session): Future[Session] = {
    // Request phase - check cache first
    handleRequestPhase(session).flatMap { s =>
      // If cache hit produced a response or early-return, stop here
      if (s.responseBody.isDefined || s.shouldReturn || s.isRejected) Future.successful(s)
      else {
        // Cache miss: call downstream, then cache the response (if any)
        next(s).flatMap { down =>
          down.responseBody match {
            case Some(resp) => handleResponsePhase(down, resp)
            case None       => Future.successful(down)
          }
        }
      }
    }
  }

  /**
   * Process with async cache strategy - check cache on request, return remote response immediately,
   * and do response validation/cache writes in the background.
   */
  protected def processCacheAsyncStrategy(session: Session): Future[Session] = {
    handleRequestPhase(session).flatMap { s =>
      if (s.responseBody.isDefined || s.shouldReturn || s.isRejected) Future.successful(s)
      else {
        next(s).map { down =>
          down.responseBody.foreach { response =>
            runResponseCacheAsync(down, response)
          }
          down
        }
      }
    }
  }

  protected def runResponseCacheAsync(session: Session, response: ByteString): Unit = {
    val _ = Future(handleResponsePhase(session, response))
      .flatMap(identity)
      .recover { case e =>
        log.warn(s"Cache: async write failed: ${e.getMessage}")
        session
      }
  }

  /**
   * Handle request phase - check cache for existing response
   */
  protected def handleRequestPhase(session: Session): Future[Session] = {
    getCacheKey(session) match {
      case Some(cacheKey) =>
        if (shouldCache(session, cacheKey)) {
          checkCache(session, cacheKey)
        } else {
          log.debug(s"Cache: SKIP: $cacheKey")
          Future.successful(session.putData("cacheKey", cacheKey))
        }

      case None =>
        log.warn(s"Cache: SKIP: no cache key (size=${session.requestBody.size})")
        Future.successful(session)
    }
  }

  /**
   * Check cache for existing response
   */
  protected def checkCache(session: Session, cacheKey: String): Future[Session] = {
    cache.get(cacheKey) match {
      case Some(entry) =>
        val now = System.currentTimeMillis()
        val entryTTL = getTTL(cacheKey)

        if (now - entry.ts < entryTTL) {
          // Cache hit - return early, skip remaining processors
          recordCacheHit(session)
          log.info(s"Req([${session.requestBody.size}]) <-- Cache([${entry.response.size}],$cacheKey)")

          Future.successful(session
            .withResponse(entry.response, ResponseSource.CACHE)
            .putData("cacheHit", true)
            .putData("fromCache", true)
            .putData("cacheKey", cacheKey)
            .returnEarly("cache_hit")  // Stop pipeline, return cached response
          )
        } else {
          // Expired - remove and miss
          cache.remove(cacheKey)
          recordCacheMiss(session)
          log.debug(s"Cache: MISS (expired): Cache([${entry.response.size}],$cacheKey)")

          Future.successful(session.putData("cacheKey", cacheKey).putData("fromCache", false))
        }

      case None =>
        // Cache miss
        recordCacheMiss(session)
        log.debug(s"Cache: MISS: $cacheKey")

        Future.successful(session.putData("cacheKey", cacheKey).putData("fromCache", false))
    }
  }

  /**
   * Handle response phase - cache successful responses
   */
  protected def handleResponsePhase(session: Session, response: ByteString): Future[Session] = {
    // Skip if response came from cache
    session.getData[Boolean]("fromCache") match {
      case Some(true) =>
        log.debug("Cache: Response already from cache, skipping write")
        return Future.successful(session)
      case _ => // Continue
    }

    // Get cache key (should have been set in request phase)
    session.getData[String]("cacheKey") match {
      case Some(cacheKey) =>
        if (shouldCacheResponse(session, response)) {
          storeInCache(session, cacheKey, response)
        } else {
          log.debug(s"Cache: SKIP: $cacheKey")
          Future.successful(session)
        }

      case None =>
        log.debug("Cache: No cache key found, skipping cache write")
        Future.successful(session)
    }
  }

  /**
   * Store response in cache
   */
  protected def storeInCache(session: Session, cacheKey: String, response: ByteString): Future[Session] = {
    val now = System.currentTimeMillis()
    cache.put(cacheKey, CacheEntry(now, response))
    log.debug(s"Cache: STORE: $cacheKey")
    Future.successful(session)
  }

  /**
   * Generate cache key from session
   * Override in subclasses for protocol-specific key generation
   * Returns None if caching should be skipped for this request
   */
  protected def getCacheKey(session: Session): Option[String] = {
    // Default: use request body as key
    Some(session.requestBody.utf8String)
  }

  /**
   * Check if this request should be cached
   * Override in subclasses for protocol-specific logic
   */
  protected def shouldCache(session: Session, cacheKey: String): Boolean = {
    // Check if request matches skip patterns
    !skipCaching.exists(pattern => cacheKey.contains(pattern))
  }

  /**
   * Check if this response should be cached
   * Override in subclasses for protocol-specific logic (e.g., skip errors)
   */
  protected def shouldCacheResponse(session: Session, response: ByteString): Boolean = {
    // Default: cache all responses
    true
  }

  /**
   * Get TTL for a specific cache key
   * Override in subclasses for dynamic TTL based on cache key
   */
  protected def getTTL(cacheKey: String): Long = ttl

  /**
   * Record cache hit in telemetry
   */
  protected def recordCacheHit(session: Session): Unit = {
    session.getData[Telemetry]("telemetry").foreach { telemetry =>
      telemetry.incCacheHits()
    }
  }

  /**
   * Record cache miss in telemetry
   */
  protected def recordCacheMiss(session: Session): Unit = {
    session.getData[Telemetry]("telemetry").foreach { telemetry =>
      telemetry.incCacheMisses()
    }
  }

  /**
   * Override toString for better logging
   */
  override def toString: String = s"${name}($effectiveStrategy,${ttl},${gcFreq})"
}

object CacheProcessor extends ProcessorConfigurable {
  def normalizeStrategy(strategy: String): String =
    strategy.trim.toLowerCase match {
      case "expire" => "cache"
      case other    => other
    }

  /**
   * Create a CacheProcessor with no caching (passthrough)
   */
  def none()(implicit ec: ExecutionContext): CacheProcessor = {
    new CacheProcessor(strategy = "none")
  }

  /**
   * Create a CacheProcessor with cache strategy.
   */
  def cache(
    ttl: Long = 30000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): CacheProcessor = {
    new CacheProcessor(
      strategy = "cache",
      ttl = ttl,
      gcFreq = gcFreq,
      skipCaching = skipCaching
    )
  }

  /**
   * Create a CacheProcessor with async response cache writes.
   */
  def cacheAsync(
    ttl: Long = 30000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): CacheProcessor = {
    new CacheProcessor(
      strategy = "cache_async",
      ttl = ttl,
      gcFreq = gcFreq,
      skipCaching = skipCaching
    )
  }

  /** Backward-compatible alias for old callers. */
  def expire(
    ttl: Long = 30000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): CacheProcessor =
    cache(ttl = ttl, gcFreq = gcFreq, skipCaching = skipCaching)

  /**
   * Build from [[CacheURI]] (`none`, `cache`, or `cache_async`; for `rpc3` use [[io.syspulse.ika.processor.rpc3.Rpc3Processor.fromUri]]).
   */
  def fromUri(c: CacheURI, skipCaching: Set[String] = Set.empty)(implicit ec: ExecutionContext): CacheProcessor = {
    c.kind match {
      case "none" =>
        none()
      case "cache" =>
        cache(ttl = c.ttl, gcFreq = c.gcFreq, skipCaching = skipCaching)
      case "cache_async" =>
        cacheAsync(ttl = c.ttl, gcFreq = c.gcFreq, skipCaching = skipCaching)
      case _ =>
        cache()
    }
  }

  override val tpe: String = "cache"

  private def cacheProcessorFromUri(c: CacheURI)(implicit ec: ExecutionContext): Processor =
    CacheProcessor.fromUri(c)

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val rawBase = if (cfg.hasPath("strategy")) cfg.getString("strategy") else "cache://"
    val base = if (rawBase.contains("://")) rawBase else s"${rawBase}://"
    val ttl = if (cfg.hasPath("ttl")) Some(cfg.getLong("ttl")) else None
    val gc = if (cfg.hasPath("gc")) Some(cfg.getLong("gc"))
    else if (cfg.hasPath("gcFreq")) Some(cfg.getLong("gcFreq"))
    else None

    val q = Seq(
      ttl.map(v => s"ttl=$v"),
      gc.map(v => s"gc=$v")
    ).flatten

    val uri = if (q.nonEmpty) s"${base}?${q.mkString("&")}" else base
    Seq(cacheProcessorFromUri(CacheURI(uri)))
  }
}
