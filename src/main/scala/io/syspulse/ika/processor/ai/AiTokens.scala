package io.syspulse.ika.processor.ai

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._
import spray.json._

import io.syspulse.ika.telemetry.{TelemetryData, TelemetryField, FieldType}

case class AiTokenPair(
  input:  AtomicLong = new AtomicLong(0L),
  output: AtomicLong = new AtomicLong(0L),
  errors: AtomicLong = new AtomicLong(0L)
)

class AiTokens extends TelemetryData {
  // provider -> model -> AiTokenPair
  private val tokenMap = new ConcurrentHashMap[String, ConcurrentHashMap[String, AiTokenPair]]()

  def addTokens(provider: String, model: String, inputTokens: Long, outputTokens: Long): Unit = {
    val p = Option(provider).getOrElse("").trim
    val m = Option(model).getOrElse("").trim
    if (p.isEmpty || m.isEmpty) return
    val models = tokenMap.computeIfAbsent(p, _ => new ConcurrentHashMap[String, AiTokenPair]())
    val pair = models.computeIfAbsent(m, _ => AiTokenPair())
    if (inputTokens > 0) pair.input.addAndGet(inputTokens)
    if (outputTokens > 0) pair.output.addAndGet(outputTokens)
  }

  def incErrors(provider: String, model: String): Unit = {
    val p = Option(provider).getOrElse("").trim
    val m = Option(model).getOrElse("").trim
    if (p.isEmpty || m.isEmpty) return
    val models = tokenMap.computeIfAbsent(p, _ => new ConcurrentHashMap[String, AiTokenPair]())
    models.computeIfAbsent(m, _ => AiTokenPair()).errors.incrementAndGet()
  }

  override def inc(field: String, delta: Long = 1L): Unit = ()
  override def set(field: String, value: Any): Unit = ()

  override val fields: Seq[TelemetryField] = Seq(
    TelemetryField("ts",            FieldType.Timestamp),
    TelemetryField("provider",      FieldType.Label),
    TelemetryField("model",         FieldType.Label),
    TelemetryField("input_tokens",  FieldType.Counter, resetOnFlush = true),
    TelemetryField("output_tokens", FieldType.Counter, resetOnFlush = true),
    TelemetryField("errors",        FieldType.Counter, resetOnFlush = true)
  )

  override def toRecords: Seq[Map[String, Any]] = {
    val ts = System.currentTimeMillis()
    tokenMap.asScala.flatMap { case (provider, models) =>
      models.asScala.map { case (model, pair) =>
        Map[String, Any](
          "ts"            -> ts,
          "provider"      -> provider,
          "model"         -> model,
          "input_tokens"  -> pair.input.get(),
          "output_tokens" -> pair.output.get(),
          "errors"        -> pair.errors.get()
        )
      }
    }.toSeq
  }

  override def toFlatKV: Map[String, String] =
    tokenMap.asScala.flatMap { case (provider, models) =>
      val p = sanitize(provider)
      models.asScala.flatMap { case (model, pair) =>
        val m = metricModelKey(provider, model)
        Map(
          s"ai.tokens.$p.$m.input"  -> pair.input.get().toString,
          s"ai.tokens.$p.$m.output" -> pair.output.get().toString
        )
      }
    }.toMap

  // Reset all resetOnFlush=true counters. input_tokens, output_tokens, errors are reset;
  // provider and model keys are kept (they identify the aggregation bucket).
  override def flush(): Unit =
    tokenMap.asScala.foreach { case (_, models) =>
      models.asScala.foreach { case (_, pair) =>
        pair.input.set(0L)
        pair.output.set(0L)
        pair.errors.set(0L)
      }
    }

  override def toCsv: String = {
    val header = fields.map(_.name).mkString(",")
    val rows = toRecords.map { r =>
      fields.map(f => r.getOrElse(f.name, "").toString).mkString(",")
    }
    (header +: rows).mkString("\n")
  }

  override def toJson: String = {
    val records = toRecords.map { r =>
      JsObject(r.map { case (k, v) =>
        k -> (v match {
          case l: Long   => JsNumber(l)
          case s: String => JsString(s)
          case n: Number => JsNumber(n.longValue())
          case _         => JsString(v.toString)
        })
      }.toMap)
    }
    JsArray(records.toVector).compactPrint
  }

  override def toString: String =
    toFlatKV.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(",")

  private def sanitize(s: String): String =
    Option(s).getOrElse("").map {
      case c if c.isLetterOrDigit            => c
      case c @ ('_' | '.' | '-')             => c
      case _                                 => '_'
    }.mkString

  private def metricModelKey(provider: String, model: String): String = {
    val p   = Option(provider).getOrElse("").trim
    val m0  = Option(model).getOrElse("").trim
    val pfx = if (p.nonEmpty) s"$p/" else ""
    val m   = if (pfx.nonEmpty && m0.startsWith(pfx)) m0.stripPrefix(pfx) else m0
    sanitize(m)
  }
}
