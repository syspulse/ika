package io.syspulse.ika.processor.uri

import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.Processor
import io.syspulse.ika.processor.impl.CacheProcessor
import io.syspulse.ika.processor.rpc3.CacheRpc3Processor

/**
 * CacheURI parses cache configuration URIs and creates CacheProcessor instances.
 *
 * Supported formats:
 * - "none://" - No caching (passthrough)
 * - "expire://" - Default expire cache (ttl=30000, gcFreq=10000)
 * - "expire://30000" - Custom ttl
 * - "expire://30000:10000" - Custom ttl and gcFreq
 *
 * RPC3-specific (with "latest" block handling):
 * - "rpc3://" - RPC3 cache with defaults (ttl=30000, ttlLatest=12000, gcFreq=10000)
 * - "rpc3://30000" - Custom ttl
 * - "rpc3://30000:12000" - Custom ttl and ttlLatest
 * - "rpc3://30000:12000:60000" - Custom ttl, ttlLatest, gcFreq
 */
object CacheURI {
  private val log = Logger("CacheURI")

  /**
   * Parse cache URI and create CacheProcessor instance
   */
  def parse(uri: String)(implicit ec: ExecutionContext): Processor = {
    uri.split("://").toList match {
      case "none" :: _ =>
        log.info("Creating CacheProcessor (none)")
        CacheProcessor.none()

      case "expire" :: Nil =>
        log.info("Creating CacheProcessor with defaults")
        CacheProcessor.expire()

      case "expire" :: params :: _ =>
        parseExpireParams(params)

      case "rpc3" :: Nil =>
        log.info("Creating CacheRpc3Processor with defaults")
        CacheRpc3Processor.expire()

      case "rpc3" :: params :: _ =>
        parseRpc3Params(params)

      case other =>
        log.warn(s"Unknown cache URI: $uri, defaulting to CacheProcessor.expire")
        CacheProcessor.expire()
    }
  }

  /**
   * Parse expire cache parameters: ttl[:gcFreq]
   */
  private def parseExpireParams(params: String)(implicit ec: ExecutionContext): Processor = {
    params.split(":").toList match {
      case ttl :: Nil =>
        val ttlValue = ttl.toLong
        log.info(s"Creating CacheProcessor with ttl=$ttlValue")
        CacheProcessor.expire(ttl = ttlValue)

      case ttl :: gcFreq :: _ =>
        val ttlValue = ttl.toLong
        val gcFreqValue = gcFreq.toLong
        log.info(s"Creating CacheProcessor with ttl=$ttlValue, gcFreq=$gcFreqValue")
        CacheProcessor.expire(ttl = ttlValue, gcFreq = gcFreqValue)

      case _ =>
        log.warn(s"Invalid expire params: $params, using defaults")
        CacheProcessor.expire()
    }
  }

  /**
   * Parse RPC3 cache parameters: ttl[:ttlLatest[:gcFreq]]
   */
  private def parseRpc3Params(params: String)(implicit ec: ExecutionContext): Processor = {
    params.split(":").toList match {
      case ttl :: Nil =>
        val ttlValue = ttl.toLong
        log.info(s"Creating CacheRpc3Processor with ttl=$ttlValue")
        CacheRpc3Processor.expire(ttl = ttlValue)

      case ttl :: ttlLatest :: Nil =>
        val ttlValue = ttl.toLong
        val ttlLatestValue = ttlLatest.toLong
        log.info(s"Creating CacheRpc3Processor with ttl=$ttlValue, ttlLatest=$ttlLatestValue")
        CacheRpc3Processor.expire(ttl = ttlValue, ttlLatest = ttlLatestValue)

      case ttl :: ttlLatest :: gcFreq :: _ =>
        val ttlValue = ttl.toLong
        val ttlLatestValue = ttlLatest.toLong
        val gcFreqValue = gcFreq.toLong
        log.info(s"Creating CacheRpc3Processor with ttl=$ttlValue, ttlLatest=$ttlLatestValue, gcFreq=$gcFreqValue")
        CacheRpc3Processor.expire(ttl = ttlValue, ttlLatest = ttlLatestValue, gcFreq = gcFreqValue)

      case _ =>
        log.warn(s"Invalid rpc3 params: $params, using defaults")
        CacheRpc3Processor.expire()
    }
  }
}
