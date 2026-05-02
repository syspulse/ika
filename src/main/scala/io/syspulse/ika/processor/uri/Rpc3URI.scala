package io.syspulse.ika.processor.uri

/**
 * RPC3 cache configuration parsed from a URI string.
 *
 * Formats:
 * - `none://` — no caching
 * - `cache://` / `rpc3://` — RPC3 cache defaults (EVM, ttl 30000, ttlLatest 12000, gcFreq 10000)
 * - `cache_async://` / `rpc3_async://` — same cache strategy, but response cache writes are asynchronous
 * - `cache://30000` — custom ttl
 * - `cache://30000,12000` — ttl and ttlLatest
 * - `cache://30000,12000,60000` — ttl, ttlLatest, gcFreq
 * - `cache://?ttl=30000&latest=12000&gc=10000` — RPC3 via query params
 * - `cache://?chain=evm` — Ethereum/EVM blockchain (default)
 * - `cache://?chain=solana` — Solana blockchain
 * - `cache://?chain=solana&ttl=30000&latest=12000` — Solana with custom TTLs
 *
 * Blockchain types (chain parameter):
 * - `evm`, `eth`, `ethereum` — Ethereum Virtual Machine chains (default)
 * - `solana`, `sol` — Solana blockchain
 *
 * `expire://` is accepted as a backward-compatible alias for `cache://`.
 * Unknown schemes default to cache with defaults.
 */
final class Rpc3URI(uri: String) extends URILike {
  private val fields: Rpc3URI.Fields = parse(uri.trim)

  def kind: String = fields.kind
  def ttl: Long = fields.ttl
  def ttlLatest: Long = fields.ttlLatest
  def gcFreq: Long = fields.gcFreq
  def ops: Map[String, String] = fields.ops

  private def parse(u: String): Rpc3URI.Fields = {
    val DEF_TTL = 30000L
    val DEF_GC = 10000L
    val DEF_TTL_LATEST = 12000L

    val (url: String, ops: Map[String, String]) = splitUrlOps(u)

    val idx = url.indexOf(PREFIX_SEP)
    if (idx < 0) {
      return Rpc3URI.Fields("cache", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
    }

    val scheme = url.substring(0, idx).toLowerCase
    val body = url.substring(idx + PREFIX_SEP.length).trim

    def commaParts(s: String): List[String] =
      s.split(',').toList.map(_.trim).filter(_.nonEmpty)

    scheme match {
      case "none" =>
        Rpc3URI.Fields("none", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
      
      case "cache" | "cache_async" | "rpc3" | "rpc3_async" | "expire" =>
        val kind =
          if (scheme == "cache_async" || scheme == "rpc3_async") "cache_async"
          else "cache"

        val ttl0 = ops.get("ttl").orElse(ops.get("t")).map(parseLong(_, DEF_TTL))
        val latest0 = ops.get("latest").orElse(ops.get("ttlLatest")).orElse(ops.get("l")).map(parseLong(_, DEF_TTL_LATEST))
        val gc0 = ops.get("gc").orElse(ops.get("g")).orElse(ops.get("gcFreq")).map(parseLong(_, DEF_GC))

        if (ttl0.isDefined || latest0.isDefined || gc0.isDefined) {
          Rpc3URI.Fields(kind, ttl0.getOrElse(DEF_TTL), latest0.getOrElse(DEF_TTL_LATEST), gc0.getOrElse(DEF_GC), ops)
        } else if (body.isEmpty) {
          Rpc3URI.Fields(kind, DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
        } else {
          commaParts(body) match {
            case ttl :: Nil =>
              Rpc3URI.Fields(kind, parseLong(ttl, DEF_TTL), DEF_TTL_LATEST, DEF_GC, ops)
            case ttl :: tl :: Nil =>
              Rpc3URI.Fields(kind, parseLong(ttl, DEF_TTL), parseLong(tl, DEF_TTL_LATEST), DEF_GC, ops)
            case ttl :: tl :: gc :: _ =>
              Rpc3URI.Fields(kind, parseLong(ttl, DEF_TTL), parseLong(tl, DEF_TTL_LATEST), parseLong(gc, DEF_GC), ops)
            case _ =>
              Rpc3URI.Fields(kind, DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
          }
        }

      case _ =>
        Rpc3URI.Fields("cache", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
    }
  }
}

object Rpc3URI {
  private[uri] case class Fields(kind: String, ttl: Long, ttlLatest: Long, gcFreq: Long, ops: Map[String, String])

  def apply(uri: String): Rpc3URI = new Rpc3URI(uri)
}
