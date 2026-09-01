package io.syspulse.ika.telemetry

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import com.typesafe.scalalogging.Logger
import akka.actor.ActorSystem
import spray.json._

/**
 * TelemetryStore manages telemetry collection and publishing to external systems.
 *
 * The store owns a Telemetry instance and a periodic scheduler. On [[start]] it schedules
 * [[publish]] at fixed [[intervalMs]] (the same mechanism every store shares); on [[stop]]
 * it cancels the scheduler. Subclasses hook resource setup/teardown via [[onStart]]/[[onStop]]
 * and define [[publish]] / [[intervalMs]].
 *
 * Implementations:
 * - TelemetryStoreFile: stdout://, stderr://, file:// (toString by default, or csv/json)
 * - TelemetryStorePrometheus: Prometheus text exposition
 * - TelemetryStoreLog: application logs
 */
abstract class TelemetryStore(implicit actorSystem: ActorSystem, ec: ExecutionContext) {
  private val schedulerLog = Logger("TelemetryStore")
  private var scheduler: Option[akka.actor.Cancellable] = None

  def telemetry: Telemetry
  def publish(): Unit

  /** Publish interval in milliseconds; values <= 0 disable scheduled publishing. */
  protected def intervalMs: Long = TelemetryStore.DefaultIntervalMs

  /** Subclass resource setup, run before the scheduler starts (e.g. open files). */
  protected def onStart(): Unit = {}

  /** Subclass resource teardown, run after the scheduler is cancelled (e.g. close files). */
  protected def onStop(): Unit = {}

  def start(): Unit = {
    onStart()
    if (intervalMs > 0 && scheduler.isEmpty) {
      scheduler = Some(
        actorSystem.scheduler.scheduleAtFixedRate(
          initialDelay = intervalMs.millis,
          interval = intervalMs.millis
        )(() => publish())
      )
      schedulerLog.info(s"${getClass.getSimpleName} publishing every ${intervalMs}ms")
    }
  }

  def stop(): Unit = {
    scheduler.foreach(_.cancel())
    scheduler = None
    onStop()
  }
}

sealed trait TelemetrySink
object TelemetrySink {
  case object Stdout extends TelemetrySink
  case object Stderr extends TelemetrySink
  final case class File(pattern: String) extends TelemetrySink
}

/** Controls when the store writes to its sink. */
sealed trait PublishPolicy
object PublishPolicy {
  /** Write on every scheduled interval regardless of new data (default). */
  case object Always extends PublishPolicy
  /** Write only when new data has been produced since the last flush. */
  case object New    extends PublishPolicy
}

final case class TelemetrySinkConfig(
  sink: TelemetrySink,
  intervalMs: Long = 60000L,
  /** None = no serialization, publish [[Telemetry.toString]] only; Some(csv|json) = serialized output */
  format: Option[String] = None,
  csvHeader: Boolean = true,
  rotateMaxBytes: Option[Long] = None,
  publishPolicy: PublishPolicy = PublishPolicy.Always,
  /** Prepend a timestamp to toString output. Defaults off for log:// (the logger adds one). */
  timestamp: Boolean = true
)

object TelemetryStore {
  private val log = Logger(this.getClass)

  val DefaultIntervalMs: Long = 60000L
  val FormatCsv: String = "csv"
  val FormatJson: String = "json"

  /** Parse optional serialization format from URI; None means Telemetry.toString. */
  def parseSerializationFormat(raw: String): Option[String] =
    raw.trim.toLowerCase match {
      case FormatCsv  => Some(FormatCsv)
      case FormatJson => Some(FormatJson)
      case _          => None
    }

  // Scalar (non-columnar) sorted KV — used by the flat CSV/JSON row.
  // Columnar data (e.g. AiTokens) is output as separate structured sections.
  private def flatKV(t: Telemetry): Seq[(String, String)] = t.toScalarFlatKV.toSeq.sortBy(_._1)

  /** CSV header row: ts,<key1>,<key2>,... */
  def csvHeaderRow(t: Telemetry): String = ("ts" +: flatKV(t).map(_._1)).mkString(",")

  /** CSV data row (single line, no header). */
  def csvDataRow(t: Telemetry): String =
    (System.currentTimeMillis().toString +: flatKV(t).map(_._2)).mkString(",")

  /**
   * CSV snapshot for file/stdout/stderr output.
   *
   * Produces a flat scalar row (ts + all counters/gauges/histograms) followed by one structured
   * section per columnar TelemetryData (e.g. AiTokens) using proper field-name columns.
   * Each columnar section: header row then one data row per dimension combination.
   */
  def toCsv(t: Telemetry, withHeader: Boolean): String = {
    val columnarData = t.getColumnarData.filter(_.toRecords.nonEmpty).toSeq.sortBy(_.getClass.getSimpleName)
    if (columnarData.nonEmpty)
      columnarData.map(data => if (withHeader) data.toCsv else data.toCsvRows.mkString("\n")).mkString("\n")
    else
      if (withHeader) s"${csvHeaderRow(t)}\n${csvDataRow(t)}" else csvDataRow(t)
  }

  /**
   * JSON snapshot for file/stdout/stderr output.
   *
   * First line: flat JSON object with scalar counters/gauges.
   * Additional lines: one JSON array per columnar TelemetryData (NDJSON style).
   */
  def toJson(t: Telemetry): String = {
    val scalarFields: Seq[(String, JsValue)] =
      ("ts" -> JsNumber(System.currentTimeMillis())) +:
        flatKV(t).map { case (k, v) => k -> JsString(v) }
    val flatLine = JsObject(scalarFields: _*).compactPrint
    val columnarLines = t.getColumnarData
      .filter(_.toRecords.nonEmpty)
      .toSeq.map(_.toJson)
    (flatLine +: columnarLines).mkString("\n")
  }

  /**
   * Format telemetry for output. No format (None) means flat "k=v" rendering, with a leading
   * `[timestamp]` only when `timestamp` is true (CSV/JSON carry their own `ts` column).
   */
  def formatOutput(t: Telemetry, format: Option[String], csvHeader: Boolean, timestamp: Boolean = true): String =
    format match {
      case Some(FormatCsv)  => toCsv(t, csvHeader)
      case Some(FormatJson) => toJson(t)
      case Some(other) =>
        log.warn(s"Unsupported telemetry format '${other}', using toString()")
        if (timestamp) t.toString else t.toKVString
      case None => if (timestamp) t.toString else t.toKVString
    }

  private def splitUrlOps(uri: String): (String, Map[String, String]) =
    uri.split("[\\?&]").toList match {
      case url :: Nil => (url, Map.empty)
      case url :: tail =>
        val vars = tail.flatMap(_.split("=", 2).toList match {
          case k :: v :: Nil => Some(k -> v)
          case _             => None
        }).toMap
        (url, vars)
      case _ => ("", Map.empty)
    }

  private def parseBool(s: String, default: Boolean): Boolean =
    s.trim.toLowerCase match {
      case "true" | "1" | "yes" => true
      case "false" | "0" | "no" => false
      case _                    => default
    }

  private def parseFormat(parts: List[String], ops: Map[String, String]): (Option[String], Boolean) = {
    ops.get("format") match {
      case Some(f) =>
        (parseSerializationFormat(f), parseBool(ops.getOrElse("header", "true"), default = true))
      case None =>
        val tokens = parts.map(_.trim.toLowerCase).filter(_.nonEmpty)
        val noHeader = tokens.exists(t => t == "noheader" || t == "nohdr")
        tokens.lastOption match {
          case Some("detailed") => (Some(FormatCsv), false)
          case Some("simple")   => (None, false)
          case _ =>
            tokens
              .find(t => t == FormatCsv || t == FormatJson)
              .map(fmt => (Some(fmt), !noHeader))
              .getOrElse((None, true))
        }
    }
  }

  /** `?timestamp=` or `?ts=` (true/false/1/0/yes/no); `default` applies when neither is present. */
  private def parseTimestamp(ops: Map[String, String], default: Boolean): Boolean =
    ops.get("timestamp").orElse(ops.get("ts")) match {
      case Some(v) => parseBool(v, default)
      case None    => default
    }

  private def parsePublishPolicy(ops: Map[String, String]): PublishPolicy =
    ops.get("publish").map(_.trim.toLowerCase) match {
      case Some("new") => PublishPolicy.New
      case _           => PublishPolicy.Always
    }

  private def parseRotate(ops: Map[String, String]): Option[Long] =
    ops.get("rotate").flatMap { r =>
      val sizeStr = if (r.contains(":")) r.split(":", 2).toList match {
        case "size" :: n :: Nil => n
        case _ :: n :: Nil      => n
        case _                  => r
      } else r
      Try(sizeStr.trim.toLong).toOption.filter(_ > 0)
    }

  def parseUri(uri: String): TelemetrySinkConfig = {
    val (url, ops) = splitUrlOps(uri.trim)
    val idx = url.indexOf("://")
    if (idx < 0) {
      return TelemetrySinkConfig(TelemetrySink.Stdout, DefaultIntervalMs, format = None, csvHeader = true)
    }

    val scheme = url.substring(0, idx).toLowerCase
    val body = url.substring(idx + 3)

    scheme match {
      case "stdout" | "stderr" =>
        val parts = if (body.isEmpty) Nil else body.split(":").toList.map(_.trim).filter(_.nonEmpty)
        val interval = parts.headOption.flatMap(p => Try(p.toLong).toOption).getOrElse(
          ops.get("interval").flatMap(s => Try(s.toLong).toOption).getOrElse(DefaultIntervalMs)
        )
        val (format, csvHeader) = parseFormat(parts.drop(1), ops)
        val sink = if (scheme == "stderr") TelemetrySink.Stderr else TelemetrySink.Stdout
        TelemetrySinkConfig(
          sink, interval, format, csvHeader,
          rotateMaxBytes = None,
          publishPolicy  = parsePublishPolicy(ops),
          timestamp      = parseTimestamp(ops, default = true)
        )

      case "file" =>
        val filePattern = body
        val interval = ops.get("interval").flatMap(s => Try(s.toLong).toOption).getOrElse(DefaultIntervalMs)
        val (format, csvHeader) = parseFormat(Nil, ops)
        TelemetrySinkConfig(
          TelemetrySink.File(filePattern),
          interval,
          format,
          csvHeader,
          rotateMaxBytes = parseRotate(ops),
          publishPolicy  = parsePublishPolicy(ops),
          timestamp      = parseTimestamp(ops, default = true)
        )

      case _ =>
        log.warn(s"Unknown telemetry URI scheme '${scheme}', using stdout")
        TelemetrySinkConfig(TelemetrySink.Stdout, DefaultIntervalMs, format = None, csvHeader = true)
    }
  }

  /**
   * Create store from URI notation.
   *
   * Examples:
   * - stdout://60000                    - toString() only (no serialization)
   * - stdout://10000?format=csv&header=true
   * - stderr://5000:json
   * - file:///var/log/ika/metrics-{yyyy}-{MM}-{dd}.csv?interval=60000&format=csv&rotate=1048576
   * - stdout://60000:detailed  (legacy: csv without header)
   * - prometheus://
   * - log://info?format=json
   */
  def fromUri(uri: String)(implicit actorSystem: ActorSystem, ec: ExecutionContext): TelemetryStore = {
    val (url, ops) = splitUrlOps(uri.trim)
    val idx = url.indexOf("://")
    if (idx < 0) {
      return new TelemetryStoreFile(parseUri(uri))
    }

    val scheme = url.substring(0, idx).toLowerCase
    val body = url.substring(idx + 3)

    val interval = ops.get("interval").flatMap(s => Try(s.toLong).toOption).getOrElse(DefaultIntervalMs)

    scheme match {
      case "prometheus" =>
        new TelemetryStorePrometheus(interval)

      case "log" =>
        // Body tokens (':'-separated) may carry, in any order: a log level, a numeric
        // interval (ms), and a format. e.g. log://1000, log://info, log://info:1000:json
        val tokens = body.split(":").toList.map(_.trim).filter(_.nonEmpty)
        val levels = Set("trace", "debug", "info", "warn", "error")
        val isNumeric = (t: String) => t.nonEmpty && t.forall(_.isDigit)
        val logInterval = tokens.find(isNumeric).flatMap(s => Try(s.toLong).toOption).getOrElse(interval)
        val level = tokens.find(t => levels.contains(t.toLowerCase)).getOrElse("info")
        val fmtTokens = tokens.filterNot(t => isNumeric(t) || levels.contains(t.toLowerCase))
        val (format, csvHeader) = parseFormat(fmtTokens, ops)
        val cfg = TelemetrySinkConfig(
          TelemetrySink.Stdout, logInterval, format, csvHeader,
          publishPolicy = parsePublishPolicy(ops),
          timestamp     = parseTimestamp(ops, default = false)
        )
        new TelemetryStoreLog(level, cfg)

      case "stdout" | "stderr" | "file" =>
        new TelemetryStoreFile(parseUri(uri))

      case _ =>
        log.warn(s"Unknown telemetry URI scheme '${scheme}', using stdout")
        new TelemetryStoreFile(parseUri(uri))
    }
  }
}
