package io.syspulse.ika.processor.ai

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._

import akka.stream.scaladsl.{Sink, Keep, Source => AkkaSource}
import akka.util.ByteString

import io.syspulse.ika.processor.{BidirectionalProcessor, Session, Processor}
import io.syspulse.ika.telemetry.Telemetry
import io.syspulse.ika.processor.util.ProcessorConfigurable
import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem

/**
 * AITokensProcessor extracts token usage from AI API responses and records to telemetry.
 *
 * This processor parses AI API responses and extracts token usage metrics.
 *
 * Request metadata is extracted dynamically from metadataUsageAttr placeholders.
 * For example, metadataUsageAttr="{tid}-{pid}-{customer_id}" reads fields
 * "tid", "pid", and "customer_id" from the root "metadata" object.
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
  private val metadataAttrTemplate = metadataUsageAttr.map(_.trim).filter(_.nonEmpty)
  private val placeholderPattern = "\\{([^{}]+)\\}".r
  private val metadataFields: Set[String] =
    metadataAttrTemplate
      .map(t => placeholderPattern.findAllMatchIn(t).map(_.group(1)).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)

  private case class Usage(inputTokens: Option[Int], outputTokens: Option[Int], totalTokens: Option[Int]) {
    def hasTokens: Boolean = inputTokens.exists(_ > 0) || outputTokens.exists(_ > 0) || totalTokens.exists(_ > 0)
  }

  private case class TelemetryTarget(provider: String, model: String)

  /**
   * Process request - extract root metadata and strip it before sending upstream.
   *
   * Expected request shape:
   * {
   *   "metadata": { ... fields referenced by metadataUsageAttr ... },
   *   ...
   * }
   */
  override def processRequest(session: Session): Future[Session] = {
    try {
      val json = session.requestBody.utf8String.parseJson.asJsObject

      val s0 = session
        .removeData("aiRequestMetadataPresent")
        .removeData("aiMetadata")

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

  // SSE state accumulated per stream chunk
  private case class SseState(buffer: String, usage: Usage, responseModel: Option[String])

  private def parseSseChunk(state: SseState, chunk: ByteString): SseState = {
    val combined = state.buffer + chunk.utf8String
    val events = combined.split("\n\n")
    // Last element may be incomplete — keep it in the buffer
    val complete = if (combined.endsWith("\n\n")) events else events.dropRight(1)
    val remaining = if (combined.endsWith("\n\n")) "" else events.lastOption.getOrElse("")

    complete.foldLeft(state.copy(buffer = remaining)) { (acc, event) =>
      event.linesIterator
        .filter(_.startsWith("data:"))
        .map(_.stripPrefix("data:").trim)
        .filterNot(_ == "[DONE]")
        .foldLeft(acc) { (s, dataStr) =>
          try {
            val json = dataStr.parseJson.asJsObject
            val usage = extractUsage(json).map(u => mergeUsage(s.usage, u)).getOrElse(s.usage)
            val responseModel = extractResponseModel(json).orElse(s.responseModel)
            s.copy(usage = usage, responseModel = responseModel)
          } catch { case _: Exception => s }
        }
    }
  }

  private def reportSseTokens(state: SseState, session: Session): Unit = {
    if (!state.usage.hasTokens) return
    recordUsage(session, state.usage, isSse = true, responseModel = state.responseModel)
  }

  /**
   * Process response - extract token usage and add to telemetry
   */
  override def processResponse(session: Session): Future[Session] = {
    // Streaming SSE path: tap the stream without buffering
    if (session.isStreaming) {
      session.responseStream match {
        case Some(stream) =>
          val telemetrySink: Sink[ByteString, Future[SseState]] =
            Sink.fold(SseState("", Usage(None, None, None), None))(parseSseChunk)

          val tappedStream: AkkaSource[ByteString, Any] =
            stream
              .alsoToMat(telemetrySink)(Keep.right)
              .mapMaterializedValue { futState =>
                futState.foreach(s => reportSseTokens(s, session))(ec)
                akka.NotUsed
              }

          Future.successful(session.copy(responseStream = Some(tappedStream)))

        case None =>
          Future.successful(session)
      }
    } else {
    // Buffered (non-streaming) path
    session.responseBody match {
      case Some(responseBody) =>
        try {
          // Parse response body as JSON
          val json = responseBody.utf8String.parseJson.asJsObject

          if (isErrorResponse(json)) {
            // Provider returned an error payload
            incAiError(session, "response_error")
          }

          // Extract usage field from normal responses or response.completed-style payloads.
          extractUsage(json) match {
            case Some(usage) =>
              val updatedSession = putUsageData(session, usage)
              recordUsage(session, usage, isSse = false, responseModel = extractResponseModel(json))
              Future.successful(updatedSession)

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
    } // end else (non-streaming)
  }

  private def recordUsage(session: Session, usage: Usage, isSse: Boolean, responseModel: Option[String]): Unit = {
    session.getData[Telemetry]("telemetry").foreach { telemetry =>
      val target = telemetryTarget(session, responseModel)
      val inTok = usage.inputTokens.getOrElse(0).toLong
      val outTok = usage.outputTokens.getOrElse(0).toLong
      val totalTok = usage.totalTokens.getOrElse((inTok + outTok).toInt)

      telemetry.addAiTokens(
        provider = target.provider,
        model = target.model,
        inputTokens = inTok,
        outputTokens = outTok
      )

      writeMetadataUsageAttr(session, usage, target, telemetry)

      val prefix = if (isSse) "SSE AI usage" else "AI usage"
      log.info(
        s"$prefix: input=$inTok, output=$outTok, total=$totalTok (provider='${target.provider}', model='${target.model}')"
      )
    }
  }

  private def writeMetadataUsageAttr(session: Session, usage: Usage, target: TelemetryTarget, telemetry: Telemetry): Unit = {
    metadataAttrTemplate.foreach { template =>
      val metaFromBody = session.getData[Map[String, String]]("aiMetadata").getOrElse(Map.empty)
      val metaFromHeaders = session.getData[Map[String, String]]("meta").getOrElse(Map.empty)
      val metadata = metaFromHeaders ++ metaFromBody

      if (metadata.nonEmpty && metadataFields.subsetOf(metadata.keySet)) {
        val attrName = interpolate(template, metadata)
          telemetry.updateUsageAttr(
            name = attrName,
            inputTokens = usage.inputTokens.getOrElse(0).toLong,
            outputTokens = usage.outputTokens.getOrElse(0).toLong,
            provider = target.provider,
            model = target.model
          )

          log.info(
            s"Telemetry metadata usage updated: attr='$attrName', metadata=${metadata}, input_tokens=${usage.inputTokens.getOrElse(0)}, output_tokens=${usage.outputTokens.getOrElse(0)}, provider='${target.provider}', model='${target.model}'"
          )
      } else if (metadata.nonEmpty) {
        val missing = metadataFields.diff(metadata.keySet).toSeq.sorted.mkString(",")
        log.debug(s"Skipping metadata usage attr '$template': missing metadata fields [$missing], metadata=$metadata")
      } else ()
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

  private def extractUsage(json: JsObject): Option[Usage] =
    extractUsageObject(json).map { usageObj =>
      val inputTokens =
        extractIntField(usageObj, "input_tokens")
          .orElse(extractIntField(usageObj, "prompt_tokens"))
      val outputTokens =
        extractIntField(usageObj, "output_tokens")
          .orElse(extractIntField(usageObj, "completion_tokens"))
      val totalTokens =
        extractIntField(usageObj, "total_tokens")
          .orElse(for { in <- inputTokens; out <- outputTokens } yield in + out)

      Usage(inputTokens, outputTokens, totalTokens)
    }

  private def extractUsageObject(json: JsObject): Option[JsObject] =
    json.fields.get("usage").collect { case o: JsObject => o }
      .orElse(
        json.fields.get("response").collect { case o: JsObject => o }
          .flatMap(_.fields.get("usage").collect { case o: JsObject => o })
      )

  private def extractResponseModel(json: JsObject): Option[String] =
    json.fields.get("model").collect { case JsString(v) if v.trim.nonEmpty => v.trim }
      .orElse(
        json.fields.get("response").collect { case o: JsObject => o }
          .flatMap(_.fields.get("model").collect { case JsString(v) if v.trim.nonEmpty => v.trim })
      )

  private def mergeUsage(prev: Usage, next: Usage): Usage =
    Usage(
      inputTokens = next.inputTokens.orElse(prev.inputTokens),
      outputTokens = next.outputTokens.orElse(prev.outputTokens),
      totalTokens = next.totalTokens.orElse(prev.totalTokens)
    )

  private def putUsageData(session: Session, usage: Usage): Session = {
    var updated = session
    usage.inputTokens.foreach(t => updated = updated.putData("promptTokens", t))
    usage.outputTokens.foreach(t => updated = updated.putData("completionTokens", t))
    usage.totalTokens.foreach(t => updated = updated.putData("totalTokens", t))
    updated
  }

  private def telemetryTarget(session: Session, responseModel: Option[String]): TelemetryTarget =
    TelemetryTarget(
      provider = session.getData[String]("provider").getOrElse("openai"),
      model = session.getData[String]("model")
        .orElse(responseModel)
        .orElse(session.getData[String]("modelUpstream"))
        .getOrElse("")
    )

  private def extractAndStripMetadata(session: Session, json: JsObject): (Session, Map[String, JsValue]) = {
    json.fields.get("metadata") match {
      case Some(metaObj: JsObject) =>
        val s0 = session.putData("aiRequestMetadataPresent", true)
        val extracted = metadataFields.flatMap { field =>
          metaObj.fields.get(field).flatMap(metadataValueToString).map(field -> _)
        }.toMap

        val s1 =
          if (extracted.nonEmpty) s0.putData("aiMetadata", extracted)
          else s0.removeData("aiMetadata")

        // Keep direct session fields for placeholders so downstream processors/tests can read them,
        // without hard-coding a particular metadata schema.
        val s2 = extracted.foldLeft(s1) { case (acc, (field, value)) =>
          value.toIntOption.map(v => acc.putData(field, v)).getOrElse(acc.putData(field, value))
        }

        (s2, json.fields - "metadata")

      case Some(other) =>
        log.warn(s"metadata field is not an object: $other")
        (session.putData("aiRequestMetadataPresent", true), json.fields - "metadata")

      case None =>
        (session.removeData("aiRequestMetadataPresent"), json.fields)
    }
  }

  private def metadataValueToString(value: JsValue): Option[String] = value match {
    case JsString(v) => Some(v)
    case JsNumber(v) => Some(v.toString)
    case JsBoolean(v) => Some(v.toString)
    case JsNull => None
    case other => Some(other.compactPrint)
  }

  private def interpolate(template: String, metadata: Map[String, String]): String =
    placeholderPattern.replaceAllIn(template, m => metadata.getOrElse(m.group(1), ""))
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
