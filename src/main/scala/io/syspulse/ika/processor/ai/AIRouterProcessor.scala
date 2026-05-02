package io.syspulse.ika.processor.ai

import scala.concurrent.{Future, ExecutionContext}
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.Logger
import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem

import spray.json._

import io.syspulse.skel.util.Util

import io.syspulse.ika.processor.{RequestProcessor, Session, Processor}
import io.syspulse.ika.processor.impl.HeaderProcessor
import io.syspulse.ika.processor.util.ProcessorConfigurable
import io.syspulse.ika.processor.uri.AiURI
/**
 * AIRouterProcessor extracts the model from AI API requests and routes to the appropriate pool.
 *
 * This processor parses OpenAI-compatible API requests, extracts the model field,
 * and maps it to a pool name for downstream load balancing.
 *
 * Model format examples:
 * - "openai/gpt-4o-mini" → pool: "openai"
 * - "anthropic/claude-3-opus" → pool: "anthropic"
 * - "gpt-4" → pool: "openai" (default)
 *
 * Session data written:
 * - "pool" (String) - The pool name for load balancing
 * - "model" (String) - The original model string
 * - "provider" (String) - The extracted provider name
 *
 * Pipeline position:
 * [...] → AIRouter → LoadBalancer → [...]
 *
 * The LoadBalancer processor reads the "pool" field to select from the appropriate pool.
 */
class AIRouterProcessor(
  providerUris: Map[String, String] = Map.empty,
  defaultProvider: String = "openai",
  modelProviderMapping: Map[String, String] = Map.empty,
  providerApiHeaderName: Map[String, String] = Map.empty,
  // NOTE: values here are FINAL header values (already formatted with api_key if needed).
  providerApiHeaderValue: Map[String, String] = Map.empty,
  providerHeaders: Map[String, Map[String, String]] = Map.empty
)(implicit ec: ExecutionContext) extends RequestProcessor {

  override val name: String = "AIRouter"

  private val log = Logger(name)

  override def toString: String =
    s"${name}(${providerUris.keySet.toSeq.sorted},def=$defaultProvider,headers=${providerApiHeaderName.keySet.toSeq.sorted})"


  // Normalize+sanitize request headers for all providers, plus optional auth injection.
  private val commonRemoveRequest: Set[String] = Set("timeout-access", "host")
  private val commonAddRequest: Map[String, String] = Map("Content-Type" -> "application/json")

  private val headerProcessors: Map[String, HeaderProcessor] = {
    val providers: Set[String] =
      (providerUris.keySet ++ providerApiHeaderName.keySet ++ providerApiHeaderValue.keySet ++ providerHeaders.keySet ++ Set(defaultProvider)).filter(_.trim.nonEmpty)

    providers.map { p =>
      val customAdds = providerHeaders.getOrElse(p, Map.empty)
      val authAdds: Map[String, String] = (for {
        hn <- providerApiHeaderName.get(p).orElse(providerApiHeaderName.get(defaultProvider))
        hv <- providerApiHeaderValue.get(p).orElse(providerApiHeaderValue.get(defaultProvider))
        hn0 = hn.trim
        hv0 = hv.trim
        if hn0.nonEmpty && hv0.nonEmpty
      } yield Map(hn0 -> hv0)).getOrElse(Map.empty)

      p -> new HeaderProcessor(
        removeRequest = commonRemoveRequest,
        addRequest = commonAddRequest ++ customAdds ++ authAdds
      )
    }.toMap
  }

  /**
   * Process request - extract model and set pool for load balancing
   */
  override def processRequest(session: Session): Future[Session] = {
    try {
      // Parse request body as JSON
      val json = session.requestBody.utf8String.parseJson.asJsObject

      // Extract model field
      json.fields.get("model") match {
        case Some(JsString(model)) =>
          val (provider, providerModel) = extractProviderAndModel(model)
          val pool = provider
          val destination = resolveDestination(provider)

          destination match {
            case Some(uri) =>
              log.info(s"Routing AI request: model='$model', provider='$provider', pool='$pool' -> destination='$uri'")

              // Rewrite request body for provider: strip optional "provider/" prefix from `model`
              // so upstream receives only the model name (e.g. "openai/gpt-4o-mini" -> "gpt-4o-mini").
              val rewritten = JsObject(json.fields.updated("model", JsString(providerModel))).compactPrint

              val s0 = session
                .withRequestBody(akka.util.ByteString(rewritten))
                .putData("model", model)
                .putData("modelUpstream", providerModel)
                .putData("provider", provider)
                .putData("pool", pool)
                // Only resolve base destination; HttpProcessor is responsible for appending uriSuffix.
                .putData("destination", uri)

              // Apply pre-built per-provider HeaderProcessor:
              // - always removes internal headers and sets Content-Type
              // - optionally injects auth header (per provider)
              headerProcessors
                .get(provider)
                .orElse(headerProcessors.get(defaultProvider))
                .map(_.processRequest(s0))
                .getOrElse(Future.successful(s0))
            case None =>
              log.warn(s"No destination configured for provider='$provider' (default='$defaultProvider')")
              Future.successful(session.reject(
                code = -32602,
                message = s"No destination configured for provider: $provider",
                processorName = name
              ))
          }

        case Some(other) =>
          log.warn(s"Model field is not a string: $other")
          Future.successful(session.reject(
            code = -32602,
            message = s"Invalid model field type: ${other.getClass.getSimpleName}",
            processorName = name
          ))

        case None =>
          log.warn("Missing model field in request")
          Future.successful(session.reject(
            code = -32602,
            message = "Missing required field: model",
            processorName = name
          ))
      }
    } catch {
      case e: spray.json.JsonParser.ParsingException =>
        log.error(s"Failed to parse request JSON: ${e.getMessage}")
        Future.successful(session.reject(
          code = -32700,
          message = s"Parse error: ${e.getMessage}",
          processorName = name
        ))

      case e: Exception =>
        log.error(s"Failed to process AI request: ${e.getMessage}", e)
        Future.successful(session.reject(
          code = -32603,
          message = s"Internal error: ${e.getMessage}",
          processorName = name,
          details = Some(e.getClass.getSimpleName)
        ))
    }
  }

  /**
   * Extract provider and provider-facing model name from model string.
   *
   * Format: "provider/model-name" → (provider, model-name)
   * If no "/" found, use model mapping or default provider.
   *
   * Examples:
   * - "openai/gpt-4o-mini" → ("openai", "gpt-4o-mini")
   * - "anthropic/claude-3-opus" → ("anthropic", "claude-3-opus")
   * - "gpt-4" → ("openai", "gpt-4")  // default provider
   */
  private def extractProviderAndModel(model: String): (String, String) = {
    val parts = model.split("/", 2)

    if (parts.length > 1) {
      (parts(0), parts(1))
    } else {
      // No provider specified, check mapping or use default
      (modelProviderMapping.getOrElse(model, defaultProvider), model)
    }
  }

  private def resolveDestination(provider: String): Option[String] =
    providerUris.get(provider).orElse(providerUris.get(defaultProvider))
}

object AIRouterProcessor extends ProcessorConfigurable {
  override val tpe: String = "ai_router"

  /**
   * Create AIRouterProcessor with default settings
   */
  def apply()(implicit ec: ExecutionContext): AIRouterProcessor = {
    new AIRouterProcessor()
  }

  /**
   * Create AIRouterProcessor with provider->URI map
   */
  def apply(
    providerUris: Map[String, String],
    defaultProvider: String = "openai"
  )(implicit ec: ExecutionContext): AIRouterProcessor = {
    new AIRouterProcessor(providerUris = providerUris, defaultProvider = defaultProvider)
  }

  def fromUri(uri: AiURI, defaultProvider: String = "openai")(implicit ec: ExecutionContext): AIRouterProcessor =
    new AIRouterProcessor(providerUris = uri.providers, defaultProvider = defaultProvider)

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val rawType = if (cfg.hasPath("type")) cfg.getString("type") else "ai_router://"
    val defaultProvider = if (cfg.hasPath("defaultProvider")) cfg.getString("defaultProvider") else "openai"

    // Support two config formats:
    // 1) type="ai_router://openai=https://...,claude=https://..."
    // 2) type="ai_router://", providers { openai { uri="...", api_key="..." } ... }
    val (providerUris, providerApiKeys) =
      if (cfg.hasPath("providers")) {
        val p = cfg.getConfig("providers")
        val names = p.root().keySet().toArray(new Array[String](0)).toSeq
        val uris = names.flatMap { n =>
          val c = p.getConfig(n)
          if (c.hasPath("uri")) Some(n -> c.getString("uri")) else None
        }.toMap
        val keys = names.flatMap { n =>
          val c = p.getConfig(n)
          if (c.hasPath("api_key")) Some(n -> c.getString("api_key")) else None
        }.toMap
        (uris, keys)
      } else {
        (AiURI(rawType).providers, Map.empty[String, String])
      }

    val (providerHeaderNames, providerHeaderValues) =
      if (cfg.hasPath("providers")) {
        val p = cfg.getConfig("providers")
        val names = p.root().keySet().toArray(new Array[String](0)).toSeq
        val hNames = names.flatMap { n =>
          val c = p.getConfig(n)
          if (c.hasPath("api_key_header_name")) Some(n -> c.getString("api_key_header_name")) else None
        }.toMap
        val hValues = names.flatMap { n =>
          val c = p.getConfig(n)
          if (c.hasPath("api_key_header_value")) Some(n -> c.getString("api_key_header_value")) else None
        }.toMap
        (hNames, hValues)
      } else {
        (Map.empty[String, String], Map.empty[String, String])
      }

    val providerHeaders: Map[String, Map[String, String]] =
      if (cfg.hasPath("providers")) {
        val p = cfg.getConfig("providers")
        val names = p.root().keySet().toArray(new Array[String](0)).toSeq
        names.flatMap { n =>
          val c = p.getConfig(n)
          if (c.hasPath("headers")) {
            val hs = c.getConfig("headers").entrySet().asScala.map { e =>
              e.getKey -> c.getString(s"headers.${e.getKey}")
            }.toMap
            Some(n -> hs)
          } else {
            None
          }
        }.toMap
      } else {
        Map.empty[String, Map[String, String]]
      }

    // Compute FINAL header values here: api_key_header_value.format(api_key)
    val providerHeaderValuesFormatted: Map[String, String] =
      providerHeaderValues.flatMap { case (provider, template) =>
        providerApiKeys.get(provider).map { apiKey =>
          provider -> template.format(apiKey)
        }
      }

    Seq(new AIRouterProcessor(
      providerUris = providerUris,
      defaultProvider = defaultProvider,
      providerApiHeaderName = providerHeaderNames,
      providerApiHeaderValue = providerHeaderValuesFormatted,
      providerHeaders = providerHeaders
    ))
  }
}
