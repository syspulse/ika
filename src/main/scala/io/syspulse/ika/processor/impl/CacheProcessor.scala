package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.concurrent
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{Processor, Session}
import io.syspulse.ika.store.ProxyData
import io.syspulse.ika.telemetry.Telemetry
import io.syspulse.skel.cron.CronFreq

/**
 * CacheProcessor provides generic request/response caching with TTL and garbage collection.
 *
 * This is a base processor that provides caching infrastructure. Subclasses can override
 * getCacheKey() and shouldCache() for protocol-specific caching logic.
 *
 * Modes:
 * - "none" - No caching (passthrough)
 * - "expire" - Time-based expiration with configurable TTL
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
  mode: String = "expire",
  ttl: Long = 30000L,
  gcFreq: Long = 10000L,
  skipCaching: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends Processor {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  override val name: String = "Cache"

  // Thread-safe cache storage
  protected case class CacheEntry(ts: Long, response: String)
  protected val cache: concurrent.Map[String, CacheEntry] = new ConcurrentHashMap[String, CacheEntry]().asScala

  // Garbage collection cron job (only for expire mode)
  private val cron: Option[CronFreq] = if (mode == "expire") {
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

  // Start GC if in expire mode
  cron.foreach(_.start())

  /**
   * Process - handles both request (cache lookup) and response (cache store)
   */
  override def process(session: Session): Future[Session] = {
    mode match {
      case "none" =>
        // No caching - pass through
        Future.successful(session)

      case "expire" =>
        processExpireMode(session)

      case _ =>
        log.warn(s"Unknown cache mode: $mode, using passthrough")
        Future.successful(session)
    }
  }

  /**
   * Process with expire mode - check cache on request, store on response
   */
  protected def processExpireMode(session: Session): Future[Session] = {
    // Check if response already set (from cache or upstream)
    session.responseBody match {
      case Some(response) =>
        // Response phase - cache the response if needed
        handleResponsePhase(session, response)

      case None =>
        // Request phase - check cache
        handleRequestPhase(session)
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
          log.debug(s"Skipping cache for: $cacheKey")
          Future.successful(session.putData("cacheKey", cacheKey))
        }

      case None =>
        log.debug("Could not generate cache key")
        Future.successful(session)
    }
  }

  /**
   * Check cache for existing response
   */
  protected def checkCache(session: Session, cacheKey: String): Future[Session] = {
    log.debug(s"Checking cache for key: $cacheKey")

    cache.get(cacheKey) match {
      case Some(entry) =>
        val now = System.currentTimeMillis()
        val entryTTL = getTTL(cacheKey)

        if (now - entry.ts < entryTTL) {
          // Cache hit - return early, skip remaining processors
          recordCacheHit(session)
          log.info(s"Cache HIT: $cacheKey")

          Future.successful(session
            .withResponse(entry.response, ProxyData.CACHE)
            .putData("cacheHit", true)
            .putData("fromCache", true)
            .putData("cacheKey", cacheKey)
            .returnEarly("cache_hit")  // Stop pipeline, return cached response
          )
        } else {
          // Expired - remove and miss
          cache.remove(cacheKey)
          recordCacheMiss(session)
          log.debug(s"Cache MISS (expired): $cacheKey")

          Future.successful(session.putData("cacheKey", cacheKey).putData("fromCache", false))
        }

      case None =>
        // Cache miss
        recordCacheMiss(session)
        log.debug(s"Cache MISS: $cacheKey")

        Future.successful(session.putData("cacheKey", cacheKey).putData("fromCache", false))
    }
  }

  /**
   * Handle response phase - cache successful responses
   */
  protected def handleResponsePhase(session: Session, response: String): Future[Session] = {
    // Skip if response came from cache
    session.getData[Boolean]("fromCache") match {
      case Some(true) =>
        log.debug("Response already from cache, skipping cache write")
        return Future.successful(session)
      case _ => // Continue
    }

    // Get cache key (should have been set in request phase)
    session.getData[String]("cacheKey") match {
      case Some(cacheKey) =>
        if (shouldCacheResponse(session, response)) {
          storeInCache(session, cacheKey, response)
        } else {
          log.debug(s"Skipping cache for response: $cacheKey")
          Future.successful(session)
        }

      case None =>
        log.debug("No cache key found, skipping cache write")
        Future.successful(session)
    }
  }

  /**
   * Store response in cache
   */
  protected def storeInCache(session: Session, cacheKey: String, response: String): Future[Session] = {
    val now = System.currentTimeMillis()
    cache.put(cacheKey, CacheEntry(now, response))
    log.info(s"Caching response: $cacheKey")
    Future.successful(session)
  }

  /**
   * Generate cache key from session
   * Override in subclasses for protocol-specific key generation
   * Returns None if caching should be skipped for this request
   */
  protected def getCacheKey(session: Session): Option[String] = {
    // Default: use request body as key
    Some(session.requestBody)
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
  protected def shouldCacheResponse(session: Session, response: String): Boolean = {
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
  override def toString: String = s"CacheProcessor(mode=$mode, ttl=${ttl}ms, gcFreq=${gcFreq}ms)"
}

object CacheProcessor {
  /**
   * Create a CacheProcessor with no caching (passthrough)
   */
  def none()(implicit ec: ExecutionContext): CacheProcessor = {
    new CacheProcessor(mode = "none")
  }

  /**
   * Create a CacheProcessor with expire mode
   */
  def expire(
    ttl: Long = 30000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): CacheProcessor = {
    new CacheProcessor(
      mode = "expire",
      ttl = ttl,
      gcFreq = gcFreq,
      skipCaching = skipCaching
    )
  }
}
