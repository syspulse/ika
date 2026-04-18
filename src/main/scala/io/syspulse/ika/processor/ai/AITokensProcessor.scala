package io.syspulse.ika.processor.ai

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._

import io.syspulse.ika.processor.{ResponseProcessor, Session, Processor}
import io.syspulse.ika.telemetry.Telemetry
import io.syspulse.ika.processor.util.ProcessorConfigurable
import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem

/**
 * AITokensProcessor extracts token usage from AI API responses and records to telemetry.
 *
 * This processor parses OpenAI-compatible API responses and extracts token usage metrics.
 *
 * Expected response format (OpenAI API):
 * {
 *   "usage": {
 *     "prompt_tokens": 10,
 *     "completion_tokens": 20,
 *     "total_tokens": 30
 *   }
 * }
 *
 * Session data written:
 * - "promptTokens" (Int) - Number of prompt tokens
 * - "completionTokens" (Int) - Number of completion tokens
 * - "totalTokens" (Int) - Total tokens used
 *
 * Telemetry metrics:
 * - Increments total tokens counter (if telemetry is available in session)
 *
 * Pipeline position:
 * [...] → HttpClient → AITokens → [...]
 */
class AITokensProcessor(implicit ec: ExecutionContext) extends ResponseProcessor {

  override val name: String = "AITokens"

  private val log = Logger(name)

  private def incAiError(session: Session, reason: String): Unit = {
    session.getData[Telemetry]("telemetry").foreach { telemetry =>
      telemetry.incErrors()
      telemetry.inc("ai.errors.total")
      telemetry.inc(s"ai.errors.${reason}")
    }
  }

  private def isErrorResponse(json: JsObject): Boolean = {
    // OpenAI-style: {"error": {...}} or sometimes {"object":"error", ...}
    json.fields.contains("error") ||
      json.fields.get("object").contains(JsString("error"))
  }

  /**
   * Process response - extract token usage and add to telemetry
   */
  override def processResponse(session: Session): Future[Session] = {
    session.responseBody match {
      case Some(responseBody) =>
        try {
          // Parse response body as JSON
          val json = responseBody.parseJson.asJsObject

          if (isErrorResponse(json)) {
            // Provider returned an error payload
            incAiError(session, "response_error")
          }

          // Extract usage field
          json.fields.get("usage") match {
            case Some(usageObj: JsObject) =>
              val promptTokens = extractIntField(usageObj, "prompt_tokens")
              val completionTokens = extractIntField(usageObj, "completion_tokens")
              val totalTokens = extractIntField(usageObj, "total_tokens")

              // Store in session
              var updatedSession = session
              promptTokens.foreach(t => updatedSession = updatedSession.putData("promptTokens", t))
              completionTokens.foreach(t => updatedSession = updatedSession.putData("completionTokens", t))
              totalTokens.foreach(t => updatedSession = updatedSession.putData("totalTokens", t))

              // Aggregate into telemetry
              session.getData[Telemetry]("telemetry").foreach { telemetry =>
                promptTokens.foreach(t => telemetry.inc("ai.tokens.prompt", t.toLong))
                completionTokens.foreach(t => telemetry.inc("ai.tokens.completion", t.toLong))
                totalTokens.foreach(t => telemetry.addTokens(t))

                val totalAll = telemetry.getCounter("ai.tokens.total")
                log.info(
                  s"AI usage: prompt=${promptTokens.getOrElse(0)}, completion=${completionTokens.getOrElse(0)}, total=${totalTokens.getOrElse(0)} (ai.tokens.total=$totalAll)"
                )
              }

              Future.successful(updatedSession)

            case Some(other) =>
              log.warn(s"Usage field is not an object: $other")
              // Count as AI error only if response isn't a known error payload (otherwise already counted)
              if (!isErrorResponse(json)) incAiError(session, "usage_invalid")
              Future.successful(session)

            case None =>
              log.warn("No usage field in response (possibly streaming or error response)")
              // If it's a JSON response (not streaming) and not an explicit error payload, count as extraction error.
              if (!isErrorResponse(json)) incAiError(session, "usage_missing")
              Future.successful(session)
          }
        } catch {
          case e: spray.json.JsonParser.ParsingException =>
            log.warn(s"Response is not valid JSON (possibly streaming): ${e.getMessage}")
            // Not an error - streaming responses aren't JSON
            Future.successful(session)

          case e: Exception =>
            log.warn(s"Failed to extract token usage: ${e.getMessage}", e)
            incAiError(session, "extract_exception")
            Future.successful(session)
        }

      case None =>
        log.warn("No response body to extract tokens from")
        incAiError(session, "empty_response")
        Future.successful(session)
    }
  }

  /**
   * Extract integer field from JSON object
   */
  private def extractIntField(obj: JsObject, fieldName: String): Option[Int] = {
    obj.fields.get(fieldName) match {
      case Some(JsNumber(value)) => Some(value.toInt)
      case Some(other) =>
        log.warn(s"Field $fieldName is not a number: $other")
        None
      case None =>
        None
    }
  }
}

object AITokensProcessor {
  /**
   * Create AITokensProcessor
   */
  def apply()(implicit ec: ExecutionContext): AITokensProcessor = {
    new AITokensProcessor()
  }
}

object AITokensProcessorConfig extends ProcessorConfigurable {
  override val tpe: String = "ai_token"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] =
    Seq(new AITokensProcessor())
}
