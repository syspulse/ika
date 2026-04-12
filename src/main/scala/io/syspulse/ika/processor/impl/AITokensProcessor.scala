package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._

import io.syspulse.ika.processor.{ResponseProcessor, Session}
import io.syspulse.ika.telemetry.Telemetry

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

  /**
   * Process response - extract token usage and add to telemetry
   */
  override def processResponse(session: Session): Future[Session] = {
    session.responseBody match {
      case Some(responseBody) =>
        try {
          // Parse response body as JSON
          val json = responseBody.parseJson.asJsObject

          // Extract usage field
          json.fields.get("usage") match {
            case Some(usageObj: JsObject) =>
              val promptTokens = extractIntField(usageObj, "prompt_tokens")
              val completionTokens = extractIntField(usageObj, "completion_tokens")
              val totalTokens = extractIntField(usageObj, "total_tokens")

              log.debug(s"Extracted token usage: prompt=$promptTokens, completion=$completionTokens, total=$totalTokens")

              // Store in session
              var updatedSession = session
              promptTokens.foreach(t => updatedSession = updatedSession.putData("promptTokens", t))
              completionTokens.foreach(t => updatedSession = updatedSession.putData("completionTokens", t))
              totalTokens.foreach(t => updatedSession = updatedSession.putData("totalTokens", t))

              // Add to telemetry if available
              totalTokens.foreach { tokens =>
                session.getData[Telemetry]("telemetry").foreach { telemetry =>
                  telemetry.addTokens(tokens)
                  log.debug(s"Recorded $tokens tokens to telemetry")
                }
              }

              Future.successful(updatedSession)

            case Some(other) =>
              log.debug(s"Usage field is not an object: $other")
              // Not an error - some responses may not have usage field
              Future.successful(session)

            case None =>
              log.debug("No usage field in response (possibly streaming or error response)")
              // Not an error - some responses don't have usage
              Future.successful(session)
          }
        } catch {
          case e: spray.json.JsonParser.ParsingException =>
            log.debug(s"Response is not valid JSON (possibly streaming): ${e.getMessage}")
            // Not an error - streaming responses aren't JSON
            Future.successful(session)

          case e: Exception =>
            log.warn(s"Failed to extract token usage: ${e.getMessage}", e)
            // Don't reject - token extraction is best-effort
            Future.successful(session)
        }

      case None =>
        log.debug("No response body to extract tokens from")
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
