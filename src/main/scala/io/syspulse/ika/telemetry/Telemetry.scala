package io.syspulse.ika.telemetry

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.Logger
import spray.json._

class Telemetry {
  private val log = Logger(s"${this.getClass.getSimpleName}")

  private val counters      = new ConcurrentHashMap[String, AtomicLong]()
  private val gauges        = new ConcurrentHashMap[String, AtomicLong]()
  private val histogramSum  = new ConcurrentHashMap[String, AtomicLong]()
  private val histogramCount= new ConcurrentHashMap[String, AtomicLong]()
  private val attributes    = new ConcurrentHashMap[String, AtomicReference[JsValue]]()

  // Registry of named TelemetryData instances (e.g. AiTokens registered as "ai.tokens").
  private val dataRegistry  = new ConcurrentHashMap[String, TelemetryData]()

  private val startTime = System.currentTimeMillis()

  // ===== TelemetryData registry =====

  def registerData(name: String, data: TelemetryData): Unit =
    dataRegistry.putIfAbsent(name, data)

  /** Thread-safe get-or-create. The type parameter is trusted by the caller. */
  def getOrRegisterData[T <: TelemetryData](name: String, create: => T): T =
    dataRegistry.computeIfAbsent(name, _ => create).asInstanceOf[T]

  /** Write all registered TelemetryData to the store, then reset resetOnFlush counters. */
  def flush(): Unit =
    dataRegistry.values().asScala.foreach(_.flush())

  // ===== Counters =====

  def inc(name: String): Unit =
    counters.computeIfAbsent(name, _ => new AtomicLong(0)).incrementAndGet()

  def inc(name: String, value: Long): Unit =
    counters.computeIfAbsent(name, _ => new AtomicLong(0)).addAndGet(value)

  def getCounter(name: String): Long =
    Option(counters.get(name)).map(_.get()).getOrElse(0L)

  // ===== Gauges =====

  def setGauge(name: String, value: Long): Unit =
    gauges.computeIfAbsent(name, _ => new AtomicLong(0)).set(value)

  def incGauge(name: String): Unit =
    gauges.computeIfAbsent(name, _ => new AtomicLong(0)).incrementAndGet()

  def decGauge(name: String): Unit =
    gauges.computeIfAbsent(name, _ => new AtomicLong(0)).decrementAndGet()

  def getGauge(name: String): Long =
    Option(gauges.get(name)).map(_.get()).getOrElse(0L)

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
   *
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
        val providerObj: JsObject = obj.fields.get(p) match {
          case Some(o: JsObject) => o
          case _                 => JsObject.empty
        }
        val modelObj: JsObject = providerObj.fields.get(m) match {
          case Some(o: JsObject) => o
          case _                 => JsObject.empty
        }
        val curIn  = modelObj.fields.get("input_tokens").map(asLong).getOrElse(0L)
        val curOut = modelObj.fields.get("output_tokens").map(asLong).getOrElse(0L)
        val updatedModelObj    = JsObject(modelObj.fields ++ Map(
          "input_tokens"  -> JsNumber(curIn  + math.max(0L, inputTokens)),
          "output_tokens" -> JsNumber(curOut + math.max(0L, outputTokens))
        ))
        val updatedProviderObj = JsObject(providerObj.fields + (m -> updatedModelObj))
        JsObject(obj.fields + (p -> updatedProviderObj))

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

  def incRequests(): Unit   = inc("requests.total")
  def incResponses(): Unit  = inc("responses.total")
  def incErrors(): Unit     = inc("errors.total")
  def incTimeouts(): Unit   = inc("errors.timeout")
  def incRetries(): Unit    = inc("retries.total")
  def incCacheHits(): Unit  = inc("cache.hits")
  def incCacheMisses(): Unit= inc("cache.misses")
  def incRejections(): Unit = inc("rejections.total")

  def addRequestBytes(bytes: Long): Unit  = if (bytes > 0) inc("requests.bytes.total", bytes)
  def addResponseBytes(bytes: Long): Unit = if (bytes > 0) inc("responses.bytes.total", bytes)

  def recordRequestTime(durationMs: Long): Unit = recordTime("request.duration", durationMs)
  def recordCacheTime(durationMs: Long): Unit   = recordTime("cache.duration", durationMs)

  // ===== Snapshots =====

  def getAllCounters: Map[String, Long] =
    counters.asScala.map { case (k, v) => k -> v.get() }.toMap

  def getAllGauges: Map[String, Long] =
    gauges.asScala.map { case (k, v) => k -> v.get() }.toMap

  def getAllHistograms: Map[String, (Long, Long, Double)] =
    histogramSum.asScala.keys.map { name =>
      val sum   = getTotalTime(name)
      val count = getTimeCount(name)
      val avg   = getAverageTime(name)
      name -> (sum, count, avg)
    }.toMap

  def getUptimeMs: Long = System.currentTimeMillis() - startTime

  def toFlatKV: Map[String, String] = {
    val base: Map[String, String] =
      getAllCounters.view.mapValues(_.toString).toMap ++
        getAllGauges.view.mapValues(_.toString).toMap ++
        getAllHistograms.map { case (name, (sum, count, avg)) =>
          Map(
            s"${name}.sum_ms" -> sum.toString,
            s"${name}.count"  -> count.toString,
            s"${name}.avg_ms" -> f"$avg%.6f"
          )
        }.foldLeft(Map.empty[String, String])(_ ++ _) ++
        Map("uptime.ms" -> getUptimeMs.toString) ++
        getAllAttributes.map { case (k, v) => s"attr.$k" -> v.compactPrint }

    val registeredKV: Map[String, String] =
      dataRegistry.asScala.values.foldLeft(Map.empty[String, String])(_ ++ _.toFlatKV)

    base ++ registeredKV
  }

  override def toString: String = {
    val timestamp = java.time.Instant.now().toString
    val kv = toFlatKV.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(",")
    s"[$timestamp] $kv"
  }

  def reset(): Unit = {
    counters.clear()
    gauges.clear()
    histogramSum.clear()
    histogramCount.clear()
    attributes.clear()
    dataRegistry.clear()
  }

  def summary(): String = {
    val sb = new StringBuilder()
    sb.append(s"=== Telemetry Summary (uptime: ${getUptimeMs}ms) ===\n")

    sb.append("\nCounters:\n")
    getAllCounters.toSeq.sortBy(_._1).foreach { case (name, value) =>
      sb.append(f"  $name%-40s: $value%,d\n")
    }

    sb.append("\nGauges:\n")
    getAllGauges.toSeq.sortBy(_._1).foreach { case (name, value) =>
      sb.append(f"  $name%-40s: $value%,d\n")
    }

    sb.append("\nHistograms:\n")
    getAllHistograms.toSeq.sortBy(_._1).foreach { case (name, (sum, count, avg)) =>
      sb.append(f"  $name%-40s: count=$count%,d, sum=${sum}%,dms, avg=${avg}%.2fms\n")
    }

    val registered = dataRegistry.asScala
    if (registered.nonEmpty) {
      sb.append("\nRegistered TelemetryData:\n")
      registered.toSeq.sortBy(_._1).foreach { case (name, data) =>
        sb.append(s"  [$name]\n")
        val kv = data.toFlatKV
        kv.toSeq.sorted.foreach { case (k, v) => sb.append(f"    $k%-44s: $v\n") }
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
