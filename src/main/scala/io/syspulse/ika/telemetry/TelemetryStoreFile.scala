package io.syspulse.ika.telemetry

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Files

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

import akka.actor.ActorSystem
import com.typesafe.scalalogging.Logger
import io.syspulse.skel.util.Util

/**
 * Writes telemetry to stdout, stderr, or a file (with optional rotation).
 *
 * With no `format` in the URI, output is [[Telemetry.toString]] only.
 * Use `format=csv` or `format=json` for serialized output.
 * File paths may include time patterns `{yyyy}`, `{MM}`, `{dd}`, `{HH}`, `{mm}`, `{ss}`;
 * rotation by time uses [[Util.nextTimestampFile]], by size uses `rotate=` query param.
 */
class TelemetryStoreFile(
  config: TelemetrySinkConfig
)(implicit actorSystem: ActorSystem, ec: ExecutionContext)
    extends TelemetryStore {

  private val log = Logger(getClass.getSimpleName)
  private var fileSink: Option[RotatingFileSink] = None

  val telemetry: Telemetry = Telemetry()

  override protected def intervalMs: Long = config.intervalMs

  def publish(): Unit = {
    if (config.publishPolicy == PublishPolicy.New && !telemetry.isDirty) return

    val line = TelemetryStore.formatOutput(telemetry, config.format, config.csvHeader, config.timestamp)
    config.sink match {
      case TelemetrySink.Stdout =>
        Console.out.println(line)
      case TelemetrySink.Stderr =>
        Console.err.println(line)
      case TelemetrySink.File(pattern) =>
        fileSink match {
          case Some(sink) =>
            writeToFile(sink, line)
          case None =>
            log.error(s"File sink not initialized for pattern: ${pattern}")
        }
    }
    telemetry.flush()
  }

  private def writeToFile(sink: RotatingFileSink, defaultLine: String): Unit =
    config.format match {
      case Some(TelemetryStore.FormatCsv) =>
        val columnarData = telemetry.getColumnarData.filter(_.toRecords.nonEmpty).toSeq
        val needsHdr = config.csvHeader && sink.needsHeader()
        if (columnarData.nonEmpty) {
          if (needsHdr) {
            columnarData.foreach(data => sink.writeLine(data.toCsvHeader))
            sink.markHeaderWritten()
          }
          columnarData.foreach(_.toCsvRows.foreach(sink.writeLine))
        } else {
          if (needsHdr) {
            sink.writeLine(TelemetryStore.csvHeaderRow(telemetry))
            sink.markHeaderWritten()
          }
          sink.writeLine(TelemetryStore.csvDataRow(telemetry))
        }
      case _ =>
        sink.writeLine(defaultLine)
    }

  override protected def onStart(): Unit =
    config.sink match {
      case TelemetrySink.File(pattern) =>
        fileSink = Some(new RotatingFileSink(pattern, config.rotateMaxBytes))
      case _ =>
        ()
    }

  override protected def onStop(): Unit = {
    fileSink.foreach(_.close())
    fileSink = None
    log.info("Stopped TelemetryStoreFile")
  }
}

/** Buffered file writer with time- and size-based rotation. */
private[telemetry] class RotatingFileSink(filePattern: String, rotateMaxBytes: Option[Long]) {
  private val log = Logger(getClass.getSimpleName)

  private var writer: Option[BufferedWriter] = None
  private var currentPath: Option[String] = None
  private var nextTs: Long = 0L
  private var bytesWritten: Long = 0L
  private var headerWritten: Boolean = false

  private val timeRotatable: Boolean = filePattern.matches(""".*[{}].*""")

  private def needRotate(): Boolean = {
    val sizeLimit = rotateMaxBytes.exists(limit => bytesWritten >= limit)
    val timeLimit = timeRotatable && nextTs != 0L && System.currentTimeMillis() >= nextTs
    writer.isEmpty || sizeLimit || timeLimit
  }

  /** @return true when the current file has not yet received a CSV header row */
  def needsHeader(): Boolean = !headerWritten

  def markHeaderWritten(): Unit =
    headerWritten = true

  def writeLine(line: String): Unit = {
    if (needRotate()) rotate()
    writer.foreach { w =>
      w.write(line)
      w.write("\n")
      bytesWritten += line.length + 1
    }
  }

  def rotate(): Unit = {
    close()
    Try(openWriter()) match {
      case Success(_) =>
        ()
      case Failure(e) =>
        log.error(s"Failed to rotate telemetry file '${filePattern}'", e)
    }
  }

  def close(): Unit = {
    writer.foreach(_.close())
    writer = None
    bytesWritten = 0L
  }

  private def openWriter(): Unit = {
    headerWritten = false
    nextTs = if (timeRotatable) Util.nextTimestampFile(filePattern) else 0L
    val now = System.currentTimeMillis()
    val path = Util.pathToFullPath(Util.toFileWithTime(filePattern, now))
    currentPath = Some(path)

    val parent = Util.getParentUri(path)
    if (parent.nonEmpty) {
      Files.createDirectories(java.nio.file.Paths.get(parent))
    }

    log.info(s"Telemetry file -> ${path}")
    writer = Some(new BufferedWriter(new FileWriter(path, true)))
    bytesWritten = 0L
  }
}
