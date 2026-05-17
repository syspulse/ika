package io.syspulse.ika.telemetry

import java.util.concurrent.atomic.AtomicLong
import spray.json._

sealed trait FieldType
object FieldType {
  case object Counter   extends FieldType // Long (monotonically increasing)
  case object Gauge     extends FieldType // Double (point-in-time value)
  case object Label     extends FieldType // String (UUID, name, key)
  case object Id        extends FieldType // Long (identifier)
  case object Timestamp extends FieldType // Long (milliseconds)
}

case class TelemetryField(name: String, tpe: FieldType, resetOnFlush: Boolean = false)

trait TelemetryData {
  def inc(field: String, delta: Long = 1L): Unit
  def set(field: String, value: Any): Unit
  def fields: Seq[TelemetryField]
  // One map per DB row; every row always includes "ts" -> Long.
  def toRecords: Seq[Map[String, Any]]
  // Flat key=value string map for embedding into Telemetry.toFlatKV (logging only).
  def toFlatKV: Map[String, String]
  // Reset fields where resetOnFlush=true. Called by Telemetry.flush() after a store write.
  def flush(): Unit
  /**
   * True when new data has been recorded since the last flush.
   * Default is true (always dirty) so scalar counters/gauges publish on every interval.
   * Override with actual tracking in types that support publish=new semantics (e.g. AiTokens).
   */
  def isDirty: Boolean = true
  /**
   * True for multi-dimensional data (e.g. AiTokens) that should be serialized using
   * toRecords/toCsvRows in file/stream output, not encoded as compound flat keys.
   * Scalar counters and gauges return false (default) and go into the single flat row.
   */
  def columnar: Boolean = false

  // CSV header row derived from fields schema.
  def toCsvHeader: String = fields.map(_.name).mkString(",")

  // CSV data rows derived from toRecords — same schema as the database write.
  def toCsvRows: Seq[String] = toRecords.map { r =>
    fields.map(f => r.getOrElse(f.name, "").toString).mkString(",")
  }

  // Full CSV (header + data rows).
  def toCsv: String = (toCsvHeader +: toCsvRows).mkString("\n")

  // JSON array of records — same structure as toRecords / DB write.
  def toJson: String = {
    val records = toRecords.map { r =>
      JsObject(r.map { case (k, v) =>
        k -> (v match {
          case l: Long   => JsNumber(l)
          case d: Double => JsNumber(d)
          case s: String => JsString(s)
          case n: Number => JsNumber(BigDecimal(n.doubleValue()))
          case _         => JsString(v.toString)
        })
      }.toMap)
    }
    JsArray(records.toVector).compactPrint
  }
}

// Single named counter. Processors create this and register it with Telemetry.
// Example: TelemetryDataCounter("responses.total")
class TelemetryDataCounter(val name: String, val resetOnFlush: Boolean = false) extends TelemetryData {
  private val _value = new AtomicLong(0L)

  def get: Long = _value.get()

  override def inc(field: String, delta: Long = 1L): Unit = _value.addAndGet(delta)
  override def set(field: String, value: Any): Unit = value match {
    case l: Long => _value.set(l)
    case i: Int  => _value.set(i.toLong)
    case _       => ()
  }

  override val fields: Seq[TelemetryField] = Seq(
    TelemetryField("ts", FieldType.Timestamp),
    TelemetryField(name, FieldType.Counter, resetOnFlush)
  )

  override def toRecords: Seq[Map[String, Any]] = Seq(Map[String, Any](
    "ts"  -> System.currentTimeMillis(),
    name  -> _value.get()
  ))

  override def toFlatKV: Map[String, String] = Map(name -> _value.get().toString)

  override def flush(): Unit = if (resetOnFlush) _value.set(0L)
}

/**
 * Holds static Long identifier fields registered by processors (e.g. tid, pid from AI request metadata).
 * Non-columnar: values appear in the flat scalar CSV/logging row via toFlatKV.
 * Not reset on flush — identifiers are set once per request context and remain until overwritten.
 */
class TelemetryDataId extends TelemetryData {
  import java.util.concurrent.ConcurrentHashMap
  import scala.jdk.CollectionConverters._

  private val values = new ConcurrentHashMap[String, Long]()

  def setLong(field: String, v: Long): Unit = values.put(field, v)
  def getLong(field: String): Option[Long]  = Option(values.get(field))

  override val columnar: Boolean = false

  override def inc(field: String, delta: Long = 1L): Unit = ()
  override def set(field: String, value: Any): Unit = value match {
    case l: Long   => values.put(field, l)
    case i: Int    => values.put(field, i.toLong)
    case s: String => s.toLongOption.foreach(values.put(field, _))
    case _         => ()
  }

  override def fields: Seq[TelemetryField] =
    values.asScala.keys.map(k => TelemetryField(k, FieldType.Id)).toSeq

  override def toRecords: Seq[Map[String, Any]] = Seq.empty

  override def toFlatKV: Map[String, String] =
    values.asScala.map { case (k, v) => k -> v.toString }.toMap

  override def flush(): Unit = ()

  // Static IDs are set-once; they never constitute "new data" for publish=new purposes.
  override def isDirty: Boolean = false
}

// Single named gauge (point-in-time value). Gauges never reset on flush.
// Example: TelemetryDataGauge("active.connections")
class TelemetryDataGauge(val name: String) extends TelemetryData {
  private val _value = new AtomicLong(0L)

  def get: Long = _value.get()

  override def inc(field: String, delta: Long = 1L): Unit = _value.addAndGet(delta)
  override def set(field: String, value: Any): Unit = value match {
    case l: Long => _value.set(l)
    case i: Int  => _value.set(i.toLong)
    case _       => ()
  }

  override val fields: Seq[TelemetryField] = Seq(
    TelemetryField("ts", FieldType.Timestamp),
    TelemetryField(name, FieldType.Gauge)  // resetOnFlush=false: gauges persist across intervals
  )

  override def toRecords: Seq[Map[String, Any]] = Seq(Map[String, Any](
    "ts"  -> System.currentTimeMillis(),
    name  -> _value.get()
  ))

  override def toFlatKV: Map[String, String] = Map(name -> _value.get().toString)

  override def flush(): Unit = ()
}
