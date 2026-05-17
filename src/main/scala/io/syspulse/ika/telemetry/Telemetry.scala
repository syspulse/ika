package io.syspulse.ika.telemetry

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.Logger
import spray.json._

class Telemetry {
  private val log = Logger(s"${this.getClass.getSimpleName}")

  // Counters and gauges are TelemetryData instances in the registry.
  // Registry key is the metric name (e.g. "responses.total", "active.connections").
  private val dataRegistry   = new ConcurrentHashMap[String, TelemetryData]()

  // Histograms keep their own maps (sum + count) — not yet wrapped as TelemetryData.
  private val histogramSum   = new ConcurrentHashMap[String, AtomicLong]()
  private val histogramCount = new ConcurrentHashMap[String, AtomicLong]()

  // Structured metadata attributes (used for per-session usage tracking).
  private val attributes     = new ConcurrentHashMap[String, AtomicReference[JsValue]]()

  private val startTime = System.currentTimeMillis()

  // ===== TelemetryData registry =====

  def registerData(name: String, data: TelemetryData): Unit =
    dataRegistry.putIfAbsent(name, data)

  /** Thread-safe get-or-create. The type parameter is trusted by the caller. */
  def getOrRegisterData[T <: TelemetryData](name: String, create: => T): T =
    dataRegistry.computeIfAbsent(name, _ => create).asInstanceOf[T]

  def getAllRegisteredData: Map[String, TelemetryData] =
    dataRegistry.asScala.toMap

  /** Columnar TelemetryData (e.g. AiTokens) — serialized as structured rows, not flat KV. */
  def getColumnarData: Iterable[TelemetryData] =
    dataRegistry.asScala.values.filter(_.columnar)

  /**
   * Flat KV for file/stream CSV output — excludes columnar data (those appear as structured sections).
   * Includes histograms, uptime, attributes, and scalar counters/gauges.
   */
  def toScalarFlatKV: Map[String, String] = {
    val histKV: Map[String, String] =
      getAllHistograms.flatMap { case (name, (sum, count, avg)) =>
        Map(
          s"${name}.sum_ms" -> sum.toString,
          s"${name}.count"  -> count.toString,
          s"${name}.avg_ms" -> f"$avg%.6f"
        )
      }
    val registeredKV: Map[String, String] =
      dataRegistry.asScala.values
        .filterNot(_.columnar)
        .foldLeft(Map.empty[String, String])(_ ++ _.toFlatKV)
    histKV ++
      Map("uptime.ms" -> getUptimeMs.toString) ++
      getAllAttributes.map { case (k, v) => s"attr.$k" -> v.compactPrint } ++
      registeredKV
  }

  /** Write registered TelemetryData to store, then reset resetOnFlush counters. */
  def flush(): Unit =
    dataRegistry.values().asScala.foreach(_.flush())

  // ===== Counters (backed by TelemetryDataCounter in the registry) =====

  def inc(name: String): Unit =
    getOrRegisterData(name, new TelemetryDataCounter(name)).inc(name)

  def inc(name: String, value: Long): Unit =
    getOrRegisterData(name, new TelemetryDataCounter(name)).inc(name, value)

  def getCounter(name: String): Long =
    Option(dataRegistry.get(name)).collect { case d: TelemetryDataCounter => d.get }.getOrElse(0L)

  def getAllCounters: Map[String, Long] =
    dataRegistry.asScala.collect { case (_, d: TelemetryDataCounter) => d.name -> d.get }.toMap

  // ===== Gauges (backed by TelemetryDataGauge in the registry) =====

  def setGauge(name: String, value: Long): Unit =
    getOrRegisterData(name, new TelemetryDataGauge(name)).set(name, value)

  def incGauge(name: String): Unit =
    getOrRegisterData(name, new TelemetryDataGauge(name)).inc(name)

  def decGauge(name: String): Unit =
    getOrRegisterData(name, new TelemetryDataGauge(name)).inc(name, -1L)

  def getGauge(name: String): Long =
    Option(dataRegistry.get(name)).collect { case d: TelemetryDataGauge => d.get }.getOrElse(0L)

  def getAllGauges: Map[String, Long] =
    dataRegistry.asScala.collect { case (_, d: TelemetryDataGauge) => d.name -> d.get }.toMap

  // ===== Histograms / Timers =====

  def recordTime(name: String, durationMs: Long): Unit = {
    histogramSum.computeIfAbsent(name, _ => new AtomicLong(0)).addAndGet(durationMs)
    histogramCount.computeIfAbsent(name, _ => new AtomicLong(0)).incrementAndGet()
  }

  def getAverageTime(name: String): Double = {
    val sum   = Option(histogramSum.get(name)).map(_.get()).getOrElse(0L)
    val count = Option(histogramCount.get(name)).map(_.get()).getOrElse(0L)
    if (count == 0) 0.0 else sum.toDouble / count.toDouble
  }

  def getTotalTime(name: String): Long =
    Option(histogramSum.get(name)).map(_.get()).getOrElse(0L)

  def getTimeCount(name: String): Long =
    Option(histogramCount.get(name)).map(_.get()).getOrElse(0L)

  def getAllHistograms: Map[String, (Long, Long, Double)] =
    histogramSum.asScala.keys.map { name =>
      val sum   = getTotalTime(name)
      val count = getTimeCount(name)
      name -> (sum, count, getAverageTime(name))
    }.toMap

  // ===== Attributes =====

  def setAttr(name: String, value: JsValue): Unit =
    attributes.computeIfAbsent(name, _ => new AtomicReference[JsValue](JsNull)).set(value)

  def updateAttr(name: String)(f: JsValue => JsValue): Unit = {
    val ref = attributes.computeIfAbsent(name, _ => new AtomicReference[JsValue](JsNull))
    var done = false
    while (!done) {
      val cur  = ref.get()
      val next = f(cur)
      done = ref.compareAndSet(cur, next)
    }
  }

  /**
   * Atomically accumulate provider/model token counts into a named attribute as nested JSON.
   * Shape: { "<provider>": { "<model>": { "input_tokens": <long>, "output_tokens": <long> } } }
   */
  def updateUsageAttr(name: String, inputTokens: Long, outputTokens: Long, provider: String, model: String): Unit = {
    def asLong(v: JsValue): Long = v match {
      case JsNumber(n) => n.toLong
      case JsString(s) => s.toLongOption.getOrElse(0L)
      case _           => 0L
    }
    val p = Option(provider).getOrElse("").trim
    val m = Option(model).getOrElse("").trim
    if (p.isEmpty || m.isEmpty) return

    def migrateFlat(obj: JsObject): JsObject = {
      val fp   = obj.fields.get("provider").collect { case JsString(v) => v }.getOrElse("")
      val fm   = obj.fields.get("model").collect { case JsString(v) => v }.getOrElse("")
      val inT  = obj.fields.get("input_tokens").map(asLong).getOrElse(0L)
      val outT = obj.fields.get("output_tokens").map(asLong).getOrElse(0L)
      if (fp.nonEmpty && fm.nonEmpty)
        JsObject(fp -> JsObject(fm -> JsObject("input_tokens" -> JsNumber(inT), "output_tokens" -> JsNumber(outT))))
      else
        JsObject(obj.fields)
    }

    updateAttr(name) {
      case obj0: JsObject =>
        val obj = if (obj0.fields.contains("provider") || obj0.fields.contains("model")) migrateFlat(obj0) else obj0
        val providerObj: JsObject = obj.fields.get(p) match { case Some(o: JsObject) => o; case _ => JsObject.empty }
        val modelObj:   JsObject = providerObj.fields.get(m) match { case Some(o: JsObject) => o; case _ => JsObject.empty }
        val curIn  = modelObj.fields.get("input_tokens").map(asLong).getOrElse(0L)
        val curOut = modelObj.fields.get("output_tokens").map(asLong).getOrElse(0L)
        val updatedModelObj    = JsObject(modelObj.fields ++ Map(
          "input_tokens"  -> JsNumber(curIn  + math.max(0L, inputTokens)),
          "output_tokens" -> JsNumber(curOut + math.max(0L, outputTokens))
        ))
        JsObject(obj.fields + (p -> JsObject(providerObj.fields + (m -> updatedModelObj))))
      case _ =>
        JsObject(p -> JsObject(m -> JsObject(
          "input_tokens"  -> JsNumber(math.max(0L, inputTokens)),
          "output_tokens" -> JsNumber(math.max(0L, outputTokens))
        )))
    }
  }

  def setAttr(name: String, value: Long): Unit   = setAttr(name, JsNumber(value))
  def setAttr(name: String, value: Int): Unit    = setAttr(name, JsNumber(value))
  def setAttr(name: String, value: String): Unit = setAttr(name, JsString(value))

  def getAttr(name: String): Option[JsValue] =
    Option(attributes.get(name)).map(_.get()).filter(_ != JsNull)

  def getAllAttributes: Map[String, JsValue] =
    attributes.asScala.map { case (k, v) => k -> v.get() }.filter(_._2 != JsNull).toMap

  // ===== Convenience metrics =====

  def incRequests(): Unit    = inc("requests.total")
  def incResponses(): Unit   = inc("responses.total")
  def incErrors(): Unit      = inc("errors.total")
  def incTimeouts(): Unit    = inc("errors.timeout")
  def incRetries(): Unit     = inc("retries.total")
  def incCacheHits(): Unit   = inc("cache.hits")
  def incCacheMisses(): Unit = inc("cache.misses")
  def incRejections(): Unit  = inc("rejections.total")

  def addRequestBytes(bytes: Long): Unit  = if (bytes > 0) inc("requests.bytes.total", bytes)
  def addResponseBytes(bytes: Long): Unit = if (bytes > 0) inc("responses.bytes.total", bytes)

  def recordRequestTime(durationMs: Long): Unit = recordTime("request.duration", durationMs)
  def recordCacheTime(durationMs: Long): Unit   = recordTime("cache.duration", durationMs)

  def getUptimeMs: Long = System.currentTimeMillis() - startTime

  // ===== Flat KV (for logging / toString only) =====

  def toFlatKV: Map[String, String] = {
    val histKV: Map[String, String] =
      getAllHistograms.flatMap { case (name, (sum, count, avg)) =>
        Map(
          s"${name}.sum_ms" -> sum.toString,
          s"${name}.count"  -> count.toString,
          s"${name}.avg_ms" -> f"$avg%.6f"
        )
      }

    val registeredKV: Map[String, String] =
      dataRegistry.asScala.values.foldLeft(Map.empty[String, String])(_ ++ _.toFlatKV)

    histKV ++
      Map("uptime.ms" -> getUptimeMs.toString) ++
      getAllAttributes.map { case (k, v) => s"attr.$k" -> v.compactPrint } ++
      registeredKV
  }

  override def toString: String = {
    val timestamp = java.time.Instant.now().toString
    val kv = toFlatKV.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(",")
    s"[$timestamp] $kv"
  }

  def reset(): Unit = {
    dataRegistry.clear()
    histogramSum.clear()
    histogramCount.clear()
    attributes.clear()
  }

  def summary(): String = {
    val sb = new StringBuilder()
    sb.append(s"=== Telemetry Summary (uptime: ${getUptimeMs}ms) ===\n")

    val counters = getAllCounters
    if (counters.nonEmpty) {
      sb.append("\nCounters:\n")
      counters.toSeq.sortBy(_._1).foreach { case (name, value) =>
        sb.append(f"  $name%-40s: $value%,d\n")
      }
    }

    val gauges = getAllGauges
    if (gauges.nonEmpty) {
      sb.append("\nGauges:\n")
      gauges.toSeq.sortBy(_._1).foreach { case (name, value) =>
        sb.append(f"  $name%-40s: $value%,d\n")
      }
    }

    val hists = getAllHistograms
    if (hists.nonEmpty) {
      sb.append("\nHistograms:\n")
      hists.toSeq.sortBy(_._1).foreach { case (name, (sum, count, avg)) =>
        sb.append(f"  $name%-40s: count=$count%,d, sum=${sum}%,dms, avg=${avg}%.2fms\n")
      }
    }

    val otherData = dataRegistry.asScala.filter {
      case (_, _: TelemetryDataCounter) => false
      case (_, _: TelemetryDataGauge)   => false
      case _                            => true
    }
    if (otherData.nonEmpty) {
      sb.append("\nRegistered TelemetryData:\n")
      otherData.toSeq.sortBy(_._1).foreach { case (name, data) =>
        sb.append(s"  [$name]\n")
        data.toFlatKV.toSeq.sorted.foreach { case (k, v) => sb.append(f"    $k%-44s: $v\n") }
      }
    }

    val attrs = getAllAttributes
    if (attrs.nonEmpty) {
      sb.append("\nAttributes:\n")
      attrs.toSeq.sortBy(_._1).foreach { case (name, value) =>
        sb.append(f"  $name%-40s: ${value.compactPrint}\n")
      }
    }

    sb.toString()
  }
}

object Telemetry {
  def apply(): Telemetry = new Telemetry()
}
