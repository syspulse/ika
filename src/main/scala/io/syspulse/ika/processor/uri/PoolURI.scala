package io.syspulse.ika.processor.uri

// lb://http://127.0.0.1:1,http://127.0.0.1:2
// sticky://http://127.0.0.1:1,http://127.0.0.1:2
// roundrobin://http://127.0.0.1:1,http://127.0.0.1:2
// random://http://127.0.0.1:1,http://127.0.0.1:2
// hash://http://127.0.0.1:1,http://127.0.0.1:2
// Tag (Id) must be supported:
// lb://tag1=http://127.0.0.1:1,http://127.0.0.1:2
// Params:
//   

final class PoolURI(uri: String) extends URILike {  
  private val fields: PoolURI.Fields = parse(uri.trim)

  def strategy: String = fields.strategy
  /** URLs parsed from the URI body (after `://`), before applying fallback. */
  def embeddedDestinations: Seq[String] = fields.embedded
  def ops: Map[String, String] = fields.ops

  def destinations(fallback: Seq[String]): Seq[String] =
    if (embeddedDestinations.nonEmpty) embeddedDestinations else fallback

  private def parse(u: String): PoolURI.Fields = {
    val (url: String, ops: Map[String, String]) = splitUrlOps(u)

    val parts = url.split(PREFIX_SEP, 2).toList
    val strat = parts.headOption.map(_.toLowerCase).filter(_.nonEmpty).getOrElse("sticky")
    val rest = parts.lift(1).map(_.trim).getOrElse("")
    val emb = parseDestinations(rest)
    PoolURI.Fields(strat, emb, ops)
  }

  private def parseDestinations(rest: String): Seq[String] = {
    val t = rest.trim
    if (t.isEmpty) return Seq.empty

    // tag1=http://a:1,http://b:2  ->  tag1:http://a:1, tag1:http://b:2
    // If individual entries have their own pool tags, keep them as-is.
    val (tagOpt, listStr) = {
      val eqIdx = t.indexOf('=')
      if (eqIdx > 0) {
        val tag = t.substring(0, eqIdx).trim
        val rhs = t.substring(eqIdx + 1).trim
        if (tag.nonEmpty && rhs.nonEmpty && !tag.contains(PREFIX_SEP)) (Some(tag), rhs) else (None, t)
      } else {
        (None, t)
      }
    }

    val urls = splitDestinations(listStr)
    tagOpt match {
      case Some(tag) =>
        urls.map { u =>
          // keep "pool:url" format (PoolProcessor expects it)
          if (u.split(":", 2).toList match {
                case _ :: url :: Nil if url.startsWith("http") => true
                case _                                         => false
              }) u
          else s"${tag}:${u}"
        }
      case None =>
        urls
    }
  }

  private def splitDestinations(rest: String): Seq[String] = {
    val t = rest.trim
    if (t.isEmpty) Seq.empty
    else if (t.contains('|'))
      t.split('|').toSeq.map(_.trim).filter(_.nonEmpty)
    else
      t.split(',').toSeq.map(_.trim).filter(_.nonEmpty)
  }
}

object PoolURI {
  private[uri] case class Fields(strategy: String, embedded: Seq[String], ops: Map[String, String])

  def apply(uri: String): PoolURI = new PoolURI(uri)
}
