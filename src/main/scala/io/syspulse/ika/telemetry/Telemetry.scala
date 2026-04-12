package io.syspulse.ika.telemetry

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import com.typesafe.scalalogging.Logger

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

  // ===== Convenience methods for common metrics =====

  def incRequests(): Unit = inc("requests.total")
  def incResponses(): Unit = inc("responses.total")
  def incErrors(): Unit = inc("errors.total")
  def incTimeouts(): Unit = inc("errors.timeout")
  def incRetries(): Unit = inc("retries.total")
  def incCacheHits(): Unit = inc("cache.hits")
  def incCacheMisses(): Unit = inc("cache.misses")
  def incRejections(): Unit = inc("rejections.total")

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

  /**
   * Get uptime in milliseconds
   */
  def getUptimeMs: Long = {
    System.currentTimeMillis() - startTime
  }

  /**
   * Reset all metrics (useful for testing)
   */
  def reset(): Unit = {
    counters.clear()
    gauges.clear()
    histogramSum.clear()
    histogramCount.clear()
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

    sb.toString()
  }
}

object Telemetry {
  /**
   * Create a new Telemetry instance
   */
  def apply(): Telemetry = new Telemetry()
}
