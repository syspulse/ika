package io.syspulse.ika.processor.core

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger

import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem
import akka.actor.Scheduler
import akka.pattern.after
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicLong
import io.syspulse.ika.processor.{RequestProcessor, Session, Processor}
import io.syspulse.ika.processor.util.ProcessorConfigurable

/**
 * ThrottleProcessor implements request throttling without blocking threads.
 *
 * This is useful for:
 * - Rate limiting to upstream services
 * - Preventing overload
 * - Respecting API quotas
 */
class ThrottleProcessor(
  throttleMs: Long,
  requests: Int = 0
)(implicit ec: ExecutionContext, scheduler: Scheduler) extends RequestProcessor {

  private val log = Logger(s"${name}")

  def name: String = "Throttle"

  override def toString: String = s"${name}(${throttleMs},${requests})"

  /**
   * Reserve the next slot and return the required delay (ms).
   * Uses a monotonic "next allowed time" to preserve throughput without blocking.
   */
  private val nextAllowedAtMs: AtomicLong = new AtomicLong(0L)
  private val windowStartMs: AtomicLong = new AtomicLong(0L)
  private val windowCount: AtomicLong = new AtomicLong(0L)

  private def reserveDelayMs(nowMs: Long): Long = {
    if (throttleMs <= 0L) return 0L

    // Burst/window throttling: allow at most `requests` within `throttleMs`.
    // If `requests` is not configured (<= 0), fall back to simple spacing logic below.
    if (requests > 0) {
      while (true) {
        val ws = windowStartMs.get()
        val wc = windowCount.get()

        // Initialize or roll window
        val ws0 =
          if (ws == 0L || nowMs - ws >= throttleMs) nowMs
          else ws

        if (ws0 != ws) {
          // try to roll window; if we win, reset count
          if (windowStartMs.compareAndSet(ws, ws0)) {
            windowCount.set(0L)
          }
          // retry with updated state
        } else {
          if (wc < requests) {
            // take a slot in current window
            if (windowCount.compareAndSet(wc, wc + 1L)) return 0L
          } else {
            // window is full: delay until next window
            val delay = (ws + throttleMs) - nowMs
            return math.max(0L, delay)
          }
        }
      }
      0L // unreachable
    }

    // Legacy throttle semantics: always delay by throttleMs (including the first request).
    throttleMs
  }

  def processRequest(session: Session): Future[Session] = {
    val now = System.currentTimeMillis()
    val delayMs = reserveDelayMs(now)

    if (delayMs <= 0L) Future.successful(session)
    else {
      log.info(s"throttling: ${delayMs}ms")
      after(delayMs.millis, scheduler)(Future.successful(session))
    }
  }
}

object ThrottleProcessor extends ProcessorConfigurable {
  override val tpe: String = "throttle"

  def apply(throttleMs: Long)(implicit ec: ExecutionContext, scheduler: Scheduler): ThrottleProcessor = {
    new ThrottleProcessor(throttleMs)
  }

  def apply(throttleMs: Long, requests: Int)(implicit ec: ExecutionContext, scheduler: Scheduler): ThrottleProcessor = {
    new ThrottleProcessor(throttleMs, requests = requests)
  }

  def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val ms0 = if (cfg.hasPath("throttle")) cfg.getLong("throttle") else 0L
    val requests0 = if (cfg.hasPath("requests")) cfg.getInt("requests") else 0

    // tps -> (requests, throttle) where throttle is a 1s window.
    // If both are present, `tps` wins as a convenience shorthand.
    val (ms, requests) =
      if (cfg.hasPath("tps")) (1000L, cfg.getInt("tps"))
      else (ms0, requests0)

    implicit val scheduler: Scheduler = actorSystem.scheduler
    Seq(new ThrottleProcessor(ms, requests = requests))
  }
}
