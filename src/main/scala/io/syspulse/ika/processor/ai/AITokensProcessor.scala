package io.syspulse.ika.processor.ai

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import spray.json._

import akka.stream.scaladsl.{Sink, Keep, Source => AkkaSource}
import akka.util.ByteString

import io.syspulse.ika.processor.{BidirectionalProcessor, Session, Processor}
import io.syspulse.ika.telemetry.{Telemetry, TelemetryDataId, TelemetryStore}
import io.syspulse.ika.processor.util.ProcessorConfigurable
import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem

/**
 * AITokensProcessor extracts token usage from AI API responses and records to its own
 * internal TelemetryStore (separate from session Telemetry).
 *
 * Session Telemetry (attached via session.putData("telemetry", ...)) is used only for
 * request-level error counters. AI token metadata (tid, pid, customer_id, provider, model,
 * input_tokens, output_tokens) is always recorded into the processor's own AiTokens instance.
 *
 * If tokensTelemetryStore is provided the processor publishes AiTokens records to it on
 * its configured interval. Typical URIs: "stdout://10000?format=csv",
 * "file:///var/log/ika/ai-tokens.csv?format=csv&interval=60000".
 *
 * Request metadata is extracted from the comma-separated field list in metadataUsageAttr.
 * For example, metadataUsageAttr="tid,pid,customer_id" extracts those three fields
 * from the root "metadata" object and stores them in the session for downstream use.
 *
 * Session data written:
 * - "promptTokens" (Int) - Number of prompt tokens
 * - "completionTokens" (Int) - Number of completion tokens
 * - "totalTokens" (Int) - Total tokens used
 *
 * Pipeline position:
 * [...] → AITokens (request metadata strip) → HttpClient → AITokens (response usage) → [...]
 */
class AITokensProcessor(
  metadataUsageAttr: Option[String] = None,
  tokensTelemetryStore: Option[TelemetryStore] = None
)(implicit ec: ExecutionContext) extends BidirectionalProcessor {

  override val name: String = "AITokens"

  private val log = Logger(name)
  private val metadataFields: Set[String] =
    metadataUsageAttr.map(_.trim).filter(_.nonEmpty)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)

  // Owned AiTokens instance — accumulates token usage across all requests.
  val aiTokens: AiTokens = new AiTokens()

  // Register aiTokens into the store's Telemetry so it is published on each interval.
  tokensTelemetryStore.foreach { store =>
    store.telemetry.registerData("ai.tokens", aiTokens)
    store.start()
  }

  def stop(): Unit = tokensTelemetryStore.foreach(_.stop())

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

      // Register tid/pid into the internal store's Telemetry as static ID fields.
      tokensTelemetryStore.foreach { store =>
        val ids = store.telemetry.getOrRegisterData("ai.ids", new TelemetryDataId())
        metadataLong(s1, "tid").foreach(ids.setLong("tid", _))
        metadataLong(s1, "pid").foreach(ids.setLong("pid", _))
      }

      if (fieldsNoMeta eq json.fields) {
        Future.successful(s1)
      } else {
        val rewritten = JsObject(fieldsNoMeta).compactPrint
        Future.successful(s1.withRequestBody(akka.util.ByteString(rewritten)))
      }
    } catch {
      case _: spray.json.JsonParser.ParsingException =>
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

  private def isErrorResponse(json: JsObject): Boolean =
    json.fields.contains("error") ||
      json.fields.get("object").contains(JsString("error"))

  // SSE state accumulated per stream chunk
  private case class SseState(buffer: String, usage: Usage, responseModel: Option[String])

  private def parseSseChunk(state: SseState, chunk: ByteString): SseState = {
    val combined = state.buffer + chunk.utf8String
    val events = combined.split("\n\n")
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

  override def processResponse(session: Session): Future[Session] = {
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
      session.responseBody match {
        case Some(responseBody) =>
          try {
            val json = responseBody.utf8String.parseJson.asJsObject

            if (isErrorResponse(json)) incAiError(session, "response_error")

            extractUsage(json) match {
              case Some(usage) =>
                val updatedSession = putUsageData(session, usage)
                recordUsage(session, usage, isSse = false, responseModel = extractResponseModel(json))
                Future.successful(updatedSession)

              case None =>
                log.warn("No usage field in response (possibly streaming or error response)")
                if (!isErrorResponse(json)) incAiError(session, "usage_missing")
                Future.successful(session)
            }
          } catch {
            case e: spray.json.JsonParser.ParsingException =>
              log.warn(s"Response is not valid JSON (possibly streaming): ${e.getMessage}")
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
  }

  private def recordUsage(session: Session, usage: Usage, isSse: Boolean, responseModel: Option[String]): Unit = {
    val target   = telemetryTarget(session, responseModel)
    val inTok    = usage.inputTokens.getOrElse(0).toLong
    val outTok   = usage.outputTokens.getOrElse(0).toLong
    val totalTok = usage.totalTokens.getOrElse((inTok + outTok).toInt)

    val customerId =
      session.getData[Map[String, String]]("aiMetadata").flatMap(_.get("customer_id"))
        .orElse(session.getData[String]("customer_id"))
        .getOrElse("")

    val tid = metadataLong(session, "tid").getOrElse(0L)
    val pid = metadataLong(session, "pid").getOrElse(0L)

    aiTokens.addTokens(tid, pid, customerId, target.provider, target.model, inTok, outTok)

    val prefix = if (isSse) "SSE AI usage" else "AI usage"
    log.info(
      s"$prefix: input=$inTok, output=$outTok, total=$totalTok (provider='${target.provider}', model='${target.model}', customer_id='$customerId')"
    )
  }

  private def extractIntField(obj: JsObject, fieldName: String): Option[Int] =
    obj.fields.get(fieldName) match {
      case Some(JsNumber(value)) => Some(value.toInt)
      case Some(other) =>
        log.warn(s"Field $fieldName is not a number: $other")
        None
      case None => None
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

  // Stored as Int when value fits (from toIntOption), otherwise as String.
  // getData[Long] on a boxed Integer throws ClassCastException so try Int first.
  private def metadataLong(session: Session, field: String): Option[Long] =
    session.getData[Int](field).map(_.toLong)
      .orElse(session.getData[String](field).flatMap(_.toLongOption))

}

object AITokensProcessor {
  def apply()(implicit ec: ExecutionContext): AITokensProcessor = new AITokensProcessor()
}

object AITokensProcessorConfig extends ProcessorConfigurable {
  override val tpe: String = "ai_token"

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val store = if (cfg.hasPath("telemetry")) {
      val rawUri = cfg.getString("telemetry")
      // Default to publish=new for AiTokens store: only write when token data was produced.
      val uri = if (!rawUri.contains("publish=")) rawUri + (if (rawUri.contains("?")) "&" else "?") + "publish=new" else rawUri
      Some(TelemetryStore.fromUri(uri))
    } else None
    Seq(new AITokensProcessor(
      metadataUsageAttr    = if (cfg.hasPath("metadataUsageAttr")) Some(cfg.getString("metadataUsageAttr")) else None,
      tokensTelemetryStore = store
    ))
  }
}
