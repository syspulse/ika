package io.syspulse.ika.processor.ai

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._

import io.syspulse.ika.processor.{BidirectionalProcessor, Session, Processor}
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
 * [...] → AITokens (request metadata strip) → HttpClient → AITokens (response usage) → [...]
 */
class AITokensProcessor(
  metadataUsageAttr: Option[String] = None
)(implicit ec: ExecutionContext) extends BidirectionalProcessor {

  override val name: String = "AITokens"

  private val log = Logger(name)

  /**
   * Process request - extract root metadata and strip it before sending upstream.
   *
   * Expected request shape:
   * {
   *   "metadata": { "pid": Int, "tid": Int, "customer_id": String },
   *   ...
   * }
   */
  override def processRequest(session: Session): Future[Session] = {
    try {
      val json = session.requestBody.utf8String.parseJson.asJsObject

      // Reset per-request metadata fields
      val s0 = session
        .removeData("aiRequestMetadataPresent")
        .removeData("pid")
        .removeData("tid")
        .removeData("customer_id")

      val (s1, fieldsNoMeta) = extractAndStripMetadata(s0, json)

      if (fieldsNoMeta eq json.fields) {
        Future.successful(s1)
      } else {
        val rewritten = JsObject(fieldsNoMeta).compactPrint
        Future.successful(s1.withRequestBody(akka.util.ByteString(rewritten)))
      }
    } catch {
      case _: spray.json.JsonParser.ParsingException =>
        // If request isn't JSON (or streaming-esque), do nothing; router/http may handle/reject later.
        Future.successful(session)
      case e: Exception =>
        log.warn(s"Failed to process request metadata: ${e.getMessage}", e)
        Future.successful(session)
    }
  }

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
          val json = responseBody.utf8String.parseJson.asJsObject

          if (isErrorResponse(json)) {
            // Provider returned an error payload
            incAiError(session, "response_error")
          }

          // Extract usage field
          json.fields.get("usage") match {
            case Some(usageObj: JsObject) =>
              // Support multiple provider schemas:
              // - OpenAI legacy: prompt_tokens / completion_tokens / total_tokens
              // - OpenAI Responses + Anthropic: input_tokens / output_tokens / total_tokens
              val promptTokens = extractIntField(usageObj, "prompt_tokens").orElse(extractIntField(usageObj, "input_tokens"))
              val completionTokens = extractIntField(usageObj, "completion_tokens").orElse(extractIntField(usageObj, "output_tokens"))
              val totalTokens =
                extractIntField(usageObj, "total_tokens")
                  .orElse(for { in <- promptTokens; out <- completionTokens } yield in + out)

              // Store in session
              var updatedSession = session
              promptTokens.foreach(t => updatedSession = updatedSession.putData("promptTokens", t))
              completionTokens.foreach(t => updatedSession = updatedSession.putData("completionTokens", t))
              totalTokens.foreach(t => updatedSession = updatedSession.putData("totalTokens", t))

              // Aggregate into telemetry
              session.getData[Telemetry]("telemetry").foreach { telemetry =>
                val provider = session.getData[String]("provider").getOrElse("openai")
                val model = session.getData[String]("model").orElse(session.getData[String]("modelUpstream")).getOrElse("")
                telemetry.addAiTokens(
                  provider = provider,
                  model = model,
                  inputTokens = promptTokens.getOrElse(0).toLong,
                  outputTokens = completionTokens.getOrElse(0).toLong
                )

                log.info(
                  s"AI usage: prompt=${promptTokens.getOrElse(0)}, completion=${completionTokens.getOrElse(0)}, total=${totalTokens.getOrElse(0)} (provider='$provider', model='$model')"
                )
              }

              // Save per-metadata usage object attribute (optional; configured via metadataUsageAttr)
              writeMetadataUsageAttr(session, promptTokens.getOrElse(0), completionTokens.getOrElse(0))

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

  private def writeMetadataUsageAttr(session: Session, inTok: Int, outTok: Int): Unit = {
    val templateOpt = metadataUsageAttr.map(_.trim).filter(_.nonEmpty)
    if (templateOpt.isEmpty) return

    val pidOpt = session.getData[Int]("pid")
    val tidOpt = session.getData[Int]("tid")
    val cidOpt = session.getData[String]("customer_id")
    (pidOpt, tidOpt, cidOpt) match {
      case (Some(pid), Some(tid), Some(customerId)) =>
        val attrName = interpolate(templateOpt.get, pid, tid, customerId)
        val provider = session.getData[String]("provider").getOrElse("openai")
        val model = session.getData[String]("model").orElse(session.getData[String]("modelUpstream")).getOrElse("")

        session.getData[Telemetry]("telemetry") match {
          case Some(t) =>
            t.updateUsageAttr(
              name = attrName,
              inputTokens = inTok.toLong,
              outputTokens = outTok.toLong,
              provider = provider,
              model = model
            )

            log.info(
              s"Telemetry metadata usage updated: attr='$attrName', tid=$tid, pid=$pid, customer_id='$customerId', input_tokens=$inTok, output_tokens=$outTok, provider='$provider', model='$model'"
            )

          case None =>
            // no telemetry attached
            ()
        }
      case _ =>
        ()
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

  private def extractAndStripMetadata(session: Session, json: JsObject): (Session, Map[String, JsValue]) = {
    json.fields.get("metadata") match {
      case Some(metaObj: JsObject) =>
        val s0 = session.putData("aiRequestMetadataPresent", true)
        val pidOpt = metaObj.fields.get("pid") collect { case JsNumber(v) => v.toInt }
        val tidOpt = metaObj.fields.get("tid") collect { case JsNumber(v) => v.toInt }
        val cidOpt = metaObj.fields.get("customer_id") collect { case JsString(v) => v }

        val s1 = cidOpt.map(v => s0.putData("customer_id", v)).getOrElse(s0)
        val s2 = tidOpt.map(v => s1.putData("tid", v)).getOrElse(s1)
        val s3 = pidOpt.map(v => s2.putData("pid", v)).getOrElse(s2)
        (s3, json.fields - "metadata")

      case Some(other) =>
        log.warn(s"metadata field is not an object: $other")
        (session.putData("aiRequestMetadataPresent", true), json.fields - "metadata")

      case None =>
        (session.removeData("aiRequestMetadataPresent"), json.fields)
    }
  }

  private def interpolate(template: String, pid: Int, tid: Int, customerId: String): String = {
    template
      .replace("{pid}", pid.toString)
      .replace("{tid}", tid.toString)
      .replace("{customer_id}", customerId)
      // Backward-compat typo support (as in example)
      .replace("{cusomter_id}", customerId)
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
    Seq(new AITokensProcessor(
      metadataUsageAttr = if (cfg.hasPath("metadataUsageAttr")) Some(cfg.getString("metadataUsageAttr")) else None
    ))
}
