package io.syspulse.ika.processor.uri

/**
 * Cache configuration parsed from a URI string (configuration only; use
 * [[io.syspulse.ika.processor.impl.CacheProcessor.fromCacheUri]] or
 * [[io.syspulse.ika.processor.rpc3.Rpc3Processor.fromUri]] to build processors).
 *
 * Formats:
 * - `none://` — no caching
 * - `expire://` — defaults (ttl 30000, gcFreq 10000)
 * - `expire://30000` — ttl
 * - `expire://30000,10000` — ttl and gcFreq
 * - `rpc3://` — RPC3 cache defaults (ttl 30000, ttlLatest 12000, gcFreq 10000)
 * - `rpc3://30000` — custom ttl
 * - `rpc3://30000,12000` — ttl and ttlLatest
 * - `rpc3://30000,12000,60000` — ttl, ttlLatest, gcFreq
 * - `expire://?ttl=30000&gc=10000` — ttl and gcFreq via query params
 * - `rpc3://?ttl=30000&latest=12000&gc=10000` — RPC3 via query params
 *
 * Unknown schemes default to expire with defaults.
 */
final class CacheURI(uri: String) extends URILike {
  private val fields: CacheURI.Fields = parse(uri.trim)

  def kind: String = fields.kind
  def ttl: Long = fields.ttl
  def ttlLatest: Long = fields.ttlLatest
  def gcFreq: Long = fields.gcFreq
  def ops: Map[String, String] = fields.ops

  private def parse(u: String): CacheURI.Fields = {
    val DEF_TTL = 30000L
    val DEF_GC = 10000L
    val DEF_TTL_LATEST = 12000L

    val (url: String, ops: Map[String, String]) = splitUrlOps(u)

    val idx = url.indexOf(PREFIX_SEP)
    if (idx < 0) {
      return CacheURI.Fields("expire", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
    }

    val scheme = url.substring(0, idx).toLowerCase
    val body = url.substring(idx + PREFIX_SEP.length).trim

    def commaParts(s: String): List[String] =
      s.split(',').toList.map(_.trim).filter(_.nonEmpty)

    scheme match {
      case "none" =>
        CacheURI.Fields("none", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)

      case "expire" =>
        val ttl0 = ops.get("ttl").orElse(ops.get("t")).map(parseLong(_, DEF_TTL))
        val gc0 = ops.get("gc").orElse(ops.get("g")).orElse(ops.get("gcFreq")).map(parseLong(_, DEF_GC))

        if (ttl0.isDefined || gc0.isDefined) {
          CacheURI.Fields("expire", ttl0.getOrElse(DEF_TTL), DEF_TTL_LATEST, gc0.getOrElse(DEF_GC), ops)
        } else if (body.isEmpty) {
          CacheURI.Fields("expire", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
        } else {
          commaParts(body) match {
            case ttl :: Nil =>
              CacheURI.Fields("expire", parseLong(ttl, DEF_TTL), DEF_TTL_LATEST, DEF_GC, ops)
            case ttl :: gc :: _ =>
              CacheURI.Fields("expire", parseLong(ttl, DEF_TTL), DEF_TTL_LATEST, parseLong(gc, DEF_GC), ops)
            case _ =>
              CacheURI.Fields("expire", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
          }
        }

      case "rpc3" =>
        val ttl0 = ops.get("ttl").orElse(ops.get("t")).map(parseLong(_, DEF_TTL))
        val latest0 = ops.get("latest").orElse(ops.get("ttlLatest")).orElse(ops.get("l")).map(parseLong(_, DEF_TTL_LATEST))
        val gc0 = ops.get("gc").orElse(ops.get("g")).orElse(ops.get("gcFreq")).map(parseLong(_, DEF_GC))

        if (ttl0.isDefined || latest0.isDefined || gc0.isDefined) {
          CacheURI.Fields("rpc3", ttl0.getOrElse(DEF_TTL), latest0.getOrElse(DEF_TTL_LATEST), gc0.getOrElse(DEF_GC), ops)
        } else if (body.isEmpty) {
          CacheURI.Fields("rpc3", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
        } else {
          commaParts(body) match {
            case ttl :: Nil =>
              CacheURI.Fields("rpc3", parseLong(ttl, DEF_TTL), DEF_TTL_LATEST, DEF_GC, ops)
            case ttl :: tl :: Nil =>
              CacheURI.Fields("rpc3", parseLong(ttl, DEF_TTL), parseLong(tl, DEF_TTL_LATEST), DEF_GC, ops)
            case ttl :: tl :: gc :: _ =>
              CacheURI.Fields("rpc3", parseLong(ttl, DEF_TTL), parseLong(tl, DEF_TTL_LATEST), parseLong(gc, DEF_GC), ops)
            case _ =>
              CacheURI.Fields("rpc3", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
          }
        }
      
      case _ =>
        CacheURI.Fields("expire", DEF_TTL, DEF_TTL_LATEST, DEF_GC, ops)
    }
  }
}

object CacheURI {
  private[uri] case class Fields(kind: String, ttl: Long, ttlLatest: Long, gcFreq: Long, ops: Map[String, String])

  def apply(uri: String): CacheURI = new CacheURI(uri)
}
