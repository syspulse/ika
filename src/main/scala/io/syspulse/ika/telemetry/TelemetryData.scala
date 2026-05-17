package io.syspulse.ika.telemetry

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
  // Flat key=value string map for embedding into Telemetry.toFlatKV.
  def toFlatKV: Map[String, String]
  // Reset fields where resetOnFlush=true. Called by Telemetry.flush() after a store write.
  def flush(): Unit
  def toCsv: String
  def toJson: String
}
