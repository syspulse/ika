package io.syspulse.ika.processor.impl

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
  protected case class CacheEntry(ts: Long, response: ByteString)
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
        next(session)

      case "expire" =>
        processExpireMode(session)

      case _ =>
        log.warn(s"Unknown cache mode: $mode, using passthrough")
        next(session)
    }
  }

  /**
   * Process with expire mode - check cache on request, store on response
   */
  protected def processExpireMode(session: Session): Future[Session] = {
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
  protected def handleResponsePhase(session: Session, response: ByteString): Future[Session] = {
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
  protected def storeInCache(session: Session, cacheKey: String, response: ByteString): Future[Session] = {
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
  override def toString: String = s"${name}($mode,${ttl},${gcFreq})"
}

object CacheProcessor extends ProcessorConfigurable {
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

  /**
   * Build from [[CacheURI]] (`none` or `expire` only; for `rpc3` use [[io.syspulse.ika.processor.rpc3.CacheRpc3Processor.fromCacheUri]]).
   */
  def fromUri(c: CacheURI, skipCaching: Set[String] = Set.empty)(implicit ec: ExecutionContext): CacheProcessor = {
    c.kind match {
      case "none" =>
        none()
      case "expire" =>
        expire(ttl = c.ttl, gcFreq = c.gcFreq, skipCaching = skipCaching)
      case _ =>
        expire()
    }
  }

  override val tpe: String = "cache"

  private def cacheProcessorFromUri(c: CacheURI)(implicit ec: ExecutionContext): Processor =
    CacheProcessor.fromUri(c)

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val rawBase = if (cfg.hasPath("strategy")) cfg.getString("strategy") else "expire://"
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
