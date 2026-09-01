package io.syspulse.ika.processor.rpc3

import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.Logger
import spray.json._
import akka.util.ByteString

/**
 * SolanaProcessor handles Solana blockchain RPC methods.
 *
 * Supports Solana-specific RPC methods:
 * - getSlot: Returns the current slot number
 * - getBlock: Returns block data by slot number
 *
 * Solana uses commitment levels instead of "latest":
 * - "finalized": Block is finalized (highest commitment)
 * - "confirmed": Block is confirmed by majority
 * - "processed": Block is processed but not confirmed
 *
 * Caching strategy:
 * - getSlot responses: cached with short TTL (hot cache)
 * - getBlock: cached by method + params; commitment-based requests also
 *   get a second entry keyed by the resolved slot from the response
 *
 * Solana response format for getBlock:
 * {
 *   "jsonrpc": "2.0",
 *   "result": {
 *     "blockHeight": 123456,
 *     "blockTime": 1234567890,
 *     "parentSlot": 123455,
 *     ...
 *   },
 *   "id": 1
 * }
 *
 * Note: Solana getBlock is called with slot number, so we extract the slot from params or metadata.
 */
class SolanaProcessor(
  strategy: String = "cache",
  ttl: Long = 10000L,
  ttlLatest: Long = 500L,
  gcFreq: Long = 11000L,
  skipCaching: Set[String] = Set.empty
)(implicit ec: ExecutionContext) extends Rpc3Processor(strategy, ttl, ttlLatest, gcFreq, skipCaching) {

  private val log = Logger(s"${this.getClass.getSimpleName}")

  override val name: String = "SolanaRpc3"

  private val commitmentLevels = Set("finalized", "confirmed", "processed")

  override protected def isBlockNumberRequest(method: String, params: List[Any]): Boolean = {
    method == "getSlot"
  }

  override protected def isBlockRequest(method: String): Boolean = {
    method == "getBlock"
  }

  /**
   * Extract slot number from Solana getBlock response or from request params
   *
   * For getBlock, the slot number is in the request params, not the response.
   * We'll try to extract from both for flexibility.
   */
  override protected def extractBlockIdentifier(response: ByteString): Option[String] = {
    try {
      val json = response.utf8String.parseJson.asJsObject

      // Try to extract slot from response (getSlot returns it directly)
      json.fields.get("result") match {
        case Some(JsNumber(slot)) =>
          // getSlot response: {"jsonrpc":"2.0","result":123456,"id":1}
          Some(slot.toLong.toString)

        case Some(resultObj: JsObject) =>
          // getBlock response might have parentSlot or blockHeight
          resultObj.fields.get("parentSlot") match {
            case Some(JsNumber(parentSlot)) =>
              // Actual slot is parentSlot + 1
              Some((parentSlot.toLong + 1).toString)
            case _ =>
              // Try blockHeight as fallback
              resultObj.fields.get("blockHeight") match {
                case Some(JsNumber(height)) => Some(height.toLong.toString)
                case _ => None
              }
          }

        case _ => None
      }
    } catch {
      case e: Exception =>
        log.warn(s"Could not parse Solana response: ${e.getMessage}")
        None
    }
  }

  override protected def replaceBlockInKey(cacheKey: String, blockIdentifier: String): String = {
    commitmentLevels.foldLeft(cacheKey) { (key, level) =>
      key.replaceAll(s""""$level"""", blockIdentifier)
         .replaceAll(level, blockIdentifier)
    }
  }

  override protected def getTTL(cacheKey: String): Long = {
    if (cacheKey.equals("""getSlot-[{"commitment":"finalized"}]""")) {
      ttlLatest
    } else {
      ttl
    }
  }

  override def toString: String = s"${name}($strategy,$ttl,$ttlLatest,$gcFreq)"
}

object SolanaProcessor {
  /**
   * Create a SolanaProcessor with no caching
   */
  def none()(implicit ec: ExecutionContext): SolanaProcessor = {
    new SolanaProcessor(strategy = "none")
  }

  /**
   * Create a SolanaProcessor with cache strategy.
   */
  def cache(
    ttl: Long = 30000L,
    ttlLatest: Long = 12000L,
    gcFreq: Long = 10000L,
    skipCaching: Set[String] = Set.empty
  )(implicit ec: ExecutionContext): SolanaProcessor = {
    new SolanaProcessor(
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
  )(implicit ec: ExecutionContext): SolanaProcessor = {
    new SolanaProcessor(
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
  )(implicit ec: ExecutionContext): SolanaProcessor =
    cache(ttl = ttl, ttlLatest = ttlLatest, gcFreq = gcFreq, skipCaching = skipCaching)
}
