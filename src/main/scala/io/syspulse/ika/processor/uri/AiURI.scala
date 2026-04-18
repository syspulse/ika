package io.syspulse.ika.processor.uri

/**
 * AI processor configuration parsed from a URI string.
 *
 * Formats:
 * - `ai_router://openai=https://api.openai.com,claude=https://api.claude.com`
 * - `ai_router://openai=https://api.openai.com|claude=https://api.claude.com`
 * - `ai_token://` (no params)
 */
final class AiURI(uri: String) extends URILike {
  private val fields: AiURI.Fields = parse(uri.trim)

  def kind: String = fields.kind
  def providers: Map[String, String] = fields.providers
  def ops: Map[String, String] = fields.ops

  private def parse(u: String): AiURI.Fields = {
    val (url: String, ops: Map[String, String]) = splitUrlOps(u)

    val idx = url.indexOf(PREFIX_SEP)
    if (idx < 0) {
      return AiURI.Fields(url.toLowerCase, Map.empty, ops)
    }

    val scheme = url.substring(0, idx).trim.toLowerCase
    val body = url.substring(idx + PREFIX_SEP.length).trim

    val entries: Seq[String] =
      if (body.isEmpty) Seq.empty
      else if (body.contains('|')) body.split('|').toSeq.map(_.trim).filter(_.nonEmpty)
      else body.split(',').toSeq.map(_.trim).filter(_.nonEmpty)

    val providers: Map[String, String] =
      entries.flatMap { e =>
        e.split("=", 2).toList match {
          case k :: v :: Nil =>
            val kk = k.trim
            val vv = v.trim
            if (kk.nonEmpty && vv.nonEmpty) Some(kk -> vv) else None
          case _ =>
            None
        }
      }.toMap

    AiURI.Fields(scheme, providers, ops)
  }
}

object AiURI {
  private[uri] case class Fields(kind: String, providers: Map[String, String], ops: Map[String, String])

  def apply(uri: String): AiURI = new AiURI(uri)
}

