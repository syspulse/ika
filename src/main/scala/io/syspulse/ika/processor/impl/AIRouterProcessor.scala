package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._

import io.syspulse.ika.processor.{RequestProcessor, Session}

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
  modelPoolMapping: Map[String, String] = Map.empty,
  defaultProvider: String = "openai"
)(implicit ec: ExecutionContext) extends RequestProcessor {

  override val name: String = "AIRouter"

  private val log = Logger(name)

  /**
   * Process request - extract model and set pool for load balancing
   */
  override def processRequest(session: Session): Future[Session] = {
    try {
      // Parse request body as JSON
      val json = session.requestBody.parseJson.asJsObject

      // Extract model field
      json.fields.get("model") match {
        case Some(JsString(model)) =>
          val (provider, pool) = extractProviderAndPool(model)

          log.debug(s"Extracted model: $model, provider: $provider, pool: $pool")

          Future.successful(session
            .putData("model", model)
            .putData("provider", provider)
            .putData("pool", pool)
          )

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
   * Extract provider and pool from model string.
   *
   * Format: "provider/model-name" → (provider, pool)
   * If no "/" found, use model as-is and apply default provider.
   *
   * Examples:
   * - "openai/gpt-4o-mini" → ("openai", "openai")
   * - "anthropic/claude-3-opus" → ("anthropic", "anthropic")
   * - "gpt-4" → ("openai", "openai")  // default provider
   */
  private def extractProviderAndPool(model: String): (String, String) = {
    val parts = model.split("/", 2)

    val provider = if (parts.length > 1) {
      parts(0)
    } else {
      // No provider specified, check mapping or use default
      modelPoolMapping.getOrElse(model, defaultProvider)
    }

    // Pool name is same as provider by default, unless explicitly mapped
    val pool = modelPoolMapping.getOrElse(model, provider)

    (provider, pool)
  }
}

object AIRouterProcessor {
  /**
   * Create AIRouterProcessor with default settings
   */
  def apply()(implicit ec: ExecutionContext): AIRouterProcessor = {
    new AIRouterProcessor()
  }

  /**
   * Create AIRouterProcessor with custom model-to-pool mapping
   */
  def apply(
    modelPoolMapping: Map[String, String],
    defaultProvider: String = "openai"
  )(implicit ec: ExecutionContext): AIRouterProcessor = {
    new AIRouterProcessor(modelPoolMapping, defaultProvider)
  }
}
