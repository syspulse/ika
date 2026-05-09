package io.syspulse.ika.telemetry

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.Logger
import spray.json._

/**
 * Thread-safe Telemetry class for collecting metrics from processors.
 *
 * All processors can increment counters, record timings, and update gauges.
 * Metrics are collected in a thread-safe manner and can be published to
 * various backends (stdout, Prometheus, etc.).
 *
 * Metric types:
 * - Counters: Monotonically increasing values (requests, errors, cache hits)
 * - Gauges: Point-in-time values (active connections, queue size)
 * - Timers: Latency measurements (request duration, processor time)
 */
class Telemetry {
  private val log = Logger(s"${this.getClass.getSimpleName}")

  // Thread-safe counters using AtomicLong
  private val counters = new ConcurrentHashMap[String, AtomicLong]()

  // Thread-safe gauges using AtomicLong
  private val gauges = new ConcurrentHashMap[String, AtomicLong]()

  // Thread-safe histogram storage (simplified - stores sum and count for averages)
  private val histogramSum = new ConcurrentHashMap[String, AtomicLong]()
  private val histogramCount = new ConcurrentHashMap[String, AtomicLong]()

  // Thread-safe attributes store (structured metadata, last-write-wins)
  private val attributes = new ConcurrentHashMap[String, AtomicReference[JsValue]]()

  // Thread-safe AI token usage accumulator: provider -> model -> {input, output}
  private case class TokenPair(input: AtomicLong, output: AtomicLong)
  private val aiTokens = new ConcurrentHashMap[String, ConcurrentHashMap[String, TokenPair]]()

  // Start time for uptime calculation
  private val startTime = System.currentTimeMillis()

  // ===== Counters =====

  /**
   * Increment a counter by 1
   */
  def inc(name: String): Unit = {
    counters.computeIfAbsent(name, _ => new AtomicLong(0)).incrementAndGet()
  }

  /**
   * Increment a counter by a specific value
   */
  def inc(name: String, value: Long): Unit = {
    counters.computeIfAbsent(name, _ => new AtomicLong(0)).addAndGet(value)
  }

  /**
   * Get counter value
   */
  def getCounter(name: String): Long = {
    Option(counters.get(name)).map(_.get()).getOrElse(0L)
  }

  // ===== Gauges =====

  /**
   * Set gauge to a specific value
   */
  def setGauge(name: String, value: Long): Unit = {
    gauges.computeIfAbsent(name, _ => new AtomicLong(0)).set(value)
  }

  /**
   * Increment gauge
   */
  def incGauge(name: String): Unit = {
    gauges.computeIfAbsent(name, _ => new AtomicLong(0)).incrementAndGet()
  }

  /**
   * Decrement gauge
   */
  def decGauge(name: String): Unit = {
    gauges.computeIfAbsent(name, _ => new AtomicLong(0)).decrementAndGet()
  }

  /**
   * Get gauge value
   */
  def getGauge(name: String): Long = {
    Option(gauges.get(name)).map(_.get()).getOrElse(0L)
  }

  // ===== Histograms/Timers =====

  /**
   * Record a timing/histogram value
   */
  def recordTime(name: String, durationMs: Long): Unit = {
    histogramSum.computeIfAbsent(name, _ => new AtomicLong(0)).addAndGet(durationMs)
    histogramCount.computeIfAbsent(name, _ => new AtomicLong(0)).incrementAndGet()
  }

  /**
   * Get average time for a histogram
   */
  def getAverageTime(name: String): Double = {
    val sum = Option(histogramSum.get(name)).map(_.get()).getOrElse(0L)
    val count = Option(histogramCount.get(name)).map(_.get()).getOrElse(0L)
    if (count == 0) 0.0 else sum.toDouble / count.toDouble
  }

  /**
   * Get total time for a histogram
   */
  def getTotalTime(name: String): Long = {
    Option(histogramSum.get(name)).map(_.get()).getOrElse(0L)
  }

  /**
   * Get count for a histogram
   */
  def getTimeCount(name: String): Long = {
    Option(histogramCount.get(name)).map(_.get()).getOrElse(0L)
  }

  // ===== Attributes =====

  /** Set an attribute value (supports structured JSON). */
  def setAttr(name: String, value: JsValue): Unit = {
    attributes.computeIfAbsent(name, _ => new AtomicReference[JsValue](JsNull)).set(value)
  }

  /**
   * Atomically update an attribute value (CAS loop).
   * Used when attribute must be accumulated, not overwritten.
   */
  def updateAttr(name: String)(f: JsValue => JsValue): Unit = {
    val ref = attributes.computeIfAbsent(name, _ => new AtomicReference[JsValue](JsNull))
    var done = false
    while (!done) {
      val cur = ref.get()
      val next = f(cur)
      done = ref.compareAndSet(cur, next)
    }
  }

  /**
   * Update metadata usage attribute as nested provider->model counters.
   *
   * Stored shape:
   * {
   *   "<provider>": {
   *     "<model>": { "input_tokens": <long>, "output_tokens": <long> }
   *   }
   * }
   *
   * If an older flat shape is present, it is migrated on next update.
   */
  def updateUsageAttr(name: String, inputTokens: Long, outputTokens: Long, provider: String, model: String): Unit = {
    def asLong(v: JsValue): Long = v match {
      case JsNumber(n) => n.toLong
      case JsString(s) => s.toLongOption.getOrElse(0L)
      case _ => 0L
    }

    val p = Option(provider).getOrElse("").trim
    val m = Option(model).getOrElse("").trim
    if (p.isEmpty || m.isEmpty) return

    def migrateFlat(obj: JsObject): JsObject = {
      val fp = obj.fields.get("provider").collect { case JsString(v) => v }.getOrElse("")
      val fm = obj.fields.get("model").collect { case JsString(v) => v }.getOrElse("")
      val inTok = obj.fields.get("input_tokens").map(asLong).getOrElse(0L)
      val outTok = obj.fields.get("output_tokens").map(asLong).getOrElse(0L)
      if (fp.nonEmpty && fm.nonEmpty) {
        JsObject(
          fp -> JsObject(
            fm -> JsObject(
              "input_tokens" -> JsNumber(inTok),
              "output_tokens" -> JsNumber(outTok)
            )
          )
        )
      } else JsObject(obj.fields) // fallback: treat as already nested or unknown
    }

    updateAttr(name) {
      case obj0: JsObject =>
        // If we detect the old flat schema, migrate to nested form.
        val obj = if (obj0.fields.contains("provider") || obj0.fields.contains("model")) migrateFlat(obj0) else obj0

        val providerObj: JsObject = obj.fields.get(p) match {
          case Some(o: JsObject) => o
          case _ => JsObject.empty
        }
        val modelObj: JsObject = providerObj.fields.get(m) match {
          case Some(o: JsObject) => o
          case _ => JsObject.empty
        }

        val curIn = modelObj.fields.get("input_tokens").map(asLong).getOrElse(0L)
        val curOut = modelObj.fields.get("output_tokens").map(asLong).getOrElse(0L)

        val updatedModelObj = JsObject(
          modelObj.fields ++ Map(
            "input_tokens" -> JsNumber(curIn + math.max(0L, inputTokens)),
            "output_tokens" -> JsNumber(curOut + math.max(0L, outputTokens))
          )
        )
        val updatedProviderObj = JsObject(providerObj.fields + (m -> updatedModelObj))
        JsObject(obj.fields + (p -> updatedProviderObj))

      case _ =>
        JsObject(
          p -> JsObject(
            m -> JsObject(
              "input_tokens" -> JsNumber(math.max(0L, inputTokens)),
              "output_tokens" -> JsNumber(math.max(0L, outputTokens))
            )
          )
        )
    }
  }

  def setAttr(name: String, value: Long): Unit = setAttr(name, JsNumber(value))
  def setAttr(name: String, value: Int): Unit = setAttr(name, JsNumber(value))
  def setAttr(name: String, value: String): Unit = setAttr(name, JsString(value))

  def getAttr(name: String): Option[JsValue] =
    Option(attributes.get(name)).map(_.get()).filter(_ != JsNull)

  def getAllAttributes: Map[String, JsValue] =
    attributes.asScala.map { case (k, v) => k -> v.get() }.filter(_._2 != JsNull).toMap

  // ===== Convenience methods for common metrics =====

  def incRequests(): Unit = inc("requests.total")
  def incResponses(): Unit = inc("responses.total")
  def incErrors(): Unit = inc("errors.total")
  def incTimeouts(): Unit = inc("errors.timeout")
  def incRetries(): Unit = inc("retries.total")
  def incCacheHits(): Unit = inc("cache.hits")
  def incCacheMisses(): Unit = inc("cache.misses")
  def incRejections(): Unit = inc("rejections.total")

  /** Accumulate AI token usage per provider/model. */
  def addAiTokens(provider: String, model: String, inputTokens: Long, outputTokens: Long): Unit = {
    val p = Option(provider).getOrElse("").trim
    val m = Option(model).getOrElse("").trim
    if (p.isEmpty || m.isEmpty) return

    val models = aiTokens.computeIfAbsent(p, _ => new ConcurrentHashMap[String, TokenPair]())
    val pair = models.computeIfAbsent(m, _ => TokenPair(new AtomicLong(0L), new AtomicLong(0L)))
    if (inputTokens > 0) pair.input.addAndGet(inputTokens)
    if (outputTokens > 0) pair.output.addAndGet(outputTokens)
  }

  /** Total request body bytes observed by the proxy (accumulated). */
  def addRequestBytes(bytes: Long): Unit = if (bytes > 0) inc("requests.bytes.total", bytes)

  /** Total response body bytes observed by the proxy (accumulated). */
  def addResponseBytes(bytes: Long): Unit = if (bytes > 0) inc("responses.bytes.total", bytes)

  def recordRequestTime(durationMs: Long): Unit = recordTime("request.duration", durationMs)
  def recordCacheTime(durationMs: Long): Unit = recordTime("cache.duration", durationMs)

  // ===== Snapshots =====

  /**
   * Get all counters as a map
   */
  def getAllCounters: Map[String, Long] = {
    counters.asScala.map { case (k, v) => k -> v.get() }.toMap
  }

  /**
   * Get all gauges as a map
   */
  def getAllGauges: Map[String, Long] = {
    gauges.asScala.map { case (k, v) => k -> v.get() }.toMap
  }

  /**
   * Get all histograms as a map with sum and count
   */
  def getAllHistograms: Map[String, (Long, Long, Double)] = {
    histogramSum.asScala.keys.map { name =>
      val sum = getTotalTime(name)
      val count = getTimeCount(name)
      val avg = getAverageTime(name)
      name -> (sum, count, avg)
    }.toMap
  }

  def getAiTokens: Map[String, Map[String, (Long, Long)]] = {
    aiTokens.asScala.map { case (provider, models) =>
      provider -> models.asScala.map { case (model, pair) =>
        model -> (pair.input.get(), pair.output.get())
      }.toMap
    }.toMap
  }

  /**
   * Get uptime in milliseconds
   */
  def getUptimeMs: Long = {
    System.currentTimeMillis() - startTime
  }

  private def sanitizeKeyPart(s: String): String =
    Option(s).getOrElse("").map {
      case c if c.isLetterOrDigit => c
      case '_' => '_'
      case '.' => '.'
      case '-' => '-'
      case _ => '_'
    }.mkString

  // For metric keys we want `ai.tokens.<provider>.<model>` even if model was stored as `provider/model`.
  private def metricModelKey(provider: String, model: String): String = {
    val p = Option(provider).getOrElse("").trim
    val m0 = Option(model).getOrElse("").trim
    val prefix = if (p.nonEmpty) s"$p/" else ""
    val m = if (prefix.nonEmpty && m0.startsWith(prefix)) m0.stripPrefix(prefix) else m0
    sanitizeKeyPart(m)
  }

  /**
   * Flatten all telemetry data to key/value strings (single-line friendly).
   * Includes counters, gauges, histograms, uptime, attributes (as compact JSON), and ai token map.
   */
  def toFlatKV: Map[String, String] = {
    val base: Map[String, String] =
      getAllCounters.view.mapValues(_.toString).toMap ++
        getAllGauges.view.mapValues(_.toString).toMap ++
        getAllHistograms.map { case (name, (sum, count, avg)) =>
          Map(
            s"${name}.sum_ms" -> sum.toString,
            s"${name}.count" -> count.toString,
            s"${name}.avg_ms" -> f"$avg%.6f"
          )
        }.foldLeft(Map.empty[String, String])(_ ++ _) ++
        Map("uptime.ms" -> getUptimeMs.toString) ++
        getAllAttributes.map { case (k, v) => s"attr.${k}" -> v.compactPrint }

    val tok: Map[String, String] =
      getAiTokens.flatMap { case (provider, models) =>
        val p = sanitizeKeyPart(provider)
        models.flatMap { case (model, (inTok, outTok)) =>
          val m = metricModelKey(provider, model)
          Map(
            s"ai.tokens.${p}.${m}.input" -> inTok.toString,
            s"ai.tokens.${p}.${m}.output" -> outTok.toString
          )
        }
      }

    base ++ tok
  }

  /**
   * Reset all metrics (useful for testing)
   */
  def reset(): Unit = {
    counters.clear()
    gauges.clear()
    histogramSum.clear()
    histogramCount.clear()
    attributes.clear()
    aiTokens.clear()
  }

  /**
   * Get a formatted summary of all metrics
   */
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

    val tok = getAiTokens
    if (tok.nonEmpty) {
      sb.append("\nAI Tokens (by provider/model):\n")
      tok.toSeq.sortBy(_._1).foreach { case (provider, models) =>
        sb.append(s"  $provider:\n")
        models.toSeq.sortBy(_._1).foreach { case (model, (inTok, outTok)) =>
          sb.append(f"    $model%-32s: input=$inTok%,d, output=$outTok%,d\n")
        }
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
  /**
   * Create a new Telemetry instance
   */
  def apply(): Telemetry = new Telemetry()
}
