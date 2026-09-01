package io.syspulse.ika.processor.ai

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import scala.jdk.CollectionConverters._

import io.syspulse.ika.telemetry.{TelemetryData, TelemetryField, FieldType}

case class AiTokenKey(tid: Long, pid: Long, customerId: String, provider: String, model: String)

class AiTokenUsage {
  val input:  AtomicLong = new AtomicLong(0L)
  val output: AtomicLong = new AtomicLong(0L)
  val errors: AtomicLong = new AtomicLong(0L)
  // lifetime total — not in fields/toRecords, never reset on flush
  val total:  AtomicLong = new AtomicLong(0L)

  private val _dirty = new AtomicBoolean(false)

  def isDirty: Boolean  = _dirty.get()
  def markDirty(): Unit = _dirty.set(true)
  def markClean(): Unit = _dirty.set(false)
}

class TelemetryDataAiTokens extends TelemetryData {
  private val tokenMap = new ConcurrentHashMap[AiTokenKey, AiTokenUsage]()

  // Derived from per-entry dirty flags — no separate class-level flag needed.
  override def isDirty: Boolean = tokenMap.asScala.values.exists(_.isDirty)

  def addTokens(tid: Long, pid: Long, customerId: String, provider: String, model: String, inputTokens: Long, outputTokens: Long): Unit = {
    val p = Option(provider).getOrElse("").trim
    val m = Option(model).getOrElse("").trim
    if (p.isEmpty || m.isEmpty) return
    val key   = AiTokenKey(tid, pid, Option(customerId).getOrElse("").trim, p, m)
    val usage = tokenMap.computeIfAbsent(key, _ => new AiTokenUsage())
    if (inputTokens > 0)  usage.input.addAndGet(inputTokens)
    if (outputTokens > 0) usage.output.addAndGet(outputTokens)
    val t = math.max(0L, inputTokens) + math.max(0L, outputTokens)
    if (t > 0) usage.total.addAndGet(t)
    usage.markDirty()
  }

  def incErrors(tid: Long, pid: Long, customerId: String, provider: String, model: String): Unit = {
    val p = Option(provider).getOrElse("").trim
    val m = Option(model).getOrElse("").trim
    if (p.isEmpty || m.isEmpty) return
    val key   = AiTokenKey(tid, pid, Option(customerId).getOrElse("").trim, p, m)
    val usage = tokenMap.computeIfAbsent(key, _ => new AiTokenUsage())
    usage.errors.incrementAndGet()
    usage.markDirty()
  }

  override def inc(field: String, delta: Long = 1L): Unit = ()
  override def set(field: String, value: Any): Unit = ()
  override val columnar: Boolean = true

  override val fields: Seq[TelemetryField] = Seq(
    TelemetryField("ts",            FieldType.Timestamp),
    TelemetryField("tid",           FieldType.Id),
    TelemetryField("pid",           FieldType.Id),
    TelemetryField("customer_id",   FieldType.Label),
    TelemetryField("provider",      FieldType.Label),
    TelemetryField("model",         FieldType.Label),
    TelemetryField("input_tokens",  FieldType.Counter, resetOnFlush = true),
    TelemetryField("output_tokens", FieldType.Counter, resetOnFlush = true),
    TelemetryField("errors",        FieldType.Counter, resetOnFlush = true)
  )

  // Only dirty entries — those updated since the last flush.
  override def toRecords: Seq[Map[String, Any]] = {
    val ts = System.currentTimeMillis()
    tokenMap.asScala
      .filter { case (_, usage) => usage.isDirty }
      .map { case (key, usage) =>
        Map[String, Any](
          "ts"            -> ts,
          "tid"           -> key.tid,
          "pid"           -> key.pid,
          "customer_id"   -> key.customerId,
          "provider"      -> key.provider,
          "model"         -> key.model,
          "input_tokens"  -> usage.input.get(),
          "output_tokens" -> usage.output.get(),
          "errors"        -> usage.errors.get()
        )
      }.toSeq
  }

  // All entries including clean ones — for logging/debugging.
  override def toFlatKV: Map[String, String] =
    tokenMap.asScala.flatMap { case (key, usage) =>
      val pfx = flatPrefix(key)
      Seq(
        s"$pfx.input"  -> usage.input.get().toString,
        s"$pfx.output" -> usage.output.get().toString,
        s"$pfx.total"  -> usage.total.get().toString
      )
    }.toMap

  // Reset all dirty entries: zero their counters and mark them clean.
  override def flush(): Unit =
    tokenMap.asScala.foreach { case (_, usage) =>
      if (usage.isDirty) {
        usage.input.set(0L)
        usage.output.set(0L)
        usage.errors.set(0L)
        usage.markClean()
      }
    }

  override def toString: String =
    toFlatKV.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(",")

  private def flatPrefix(key: AiTokenKey): String = {
    val p   = sanitize(key.provider)
    val m   = metricModelKey(key.provider, key.model)
    val cid = if (key.customerId.nonEmpty) sanitize(key.customerId) else "_"
    s"ai.tokens.${key.tid}.${key.pid}.$cid.$p.$m"
  }

  private def sanitize(s: String): String =
    Option(s).getOrElse("").map {
      case c if c.isLetterOrDigit    => c
      case c @ ('_' | '.' | '-')     => c
      case _                         => '_'
    }.mkString

  private def metricModelKey(provider: String, model: String): String = {
    val p   = Option(provider).getOrElse("").trim
    val m0  = Option(model).getOrElse("").trim
    val pfx = if (p.nonEmpty) s"$p/" else ""
    val m   = if (pfx.nonEmpty && m0.startsWith(pfx)) m0.stripPrefix(pfx) else m0
    sanitize(m)
  }
}
