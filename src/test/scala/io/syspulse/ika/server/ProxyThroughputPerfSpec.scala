package io.syspulse.ika.server

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import io.syspulse.ika.processor.ProcessorPipeline
import io.syspulse.ika.processor.core.{HeaderProcessor, HttpProcessor, PoolProcessor, RetryProcessor}
import io.syspulse.ika.store.ProxyStorePipeline

/**
 * Parameterized integration throughput test.
 *
 * Each scenario exercises the full pipeline:
 *   HeaderProcessor → PoolProcessor(round-robin) → RetryProcessor → HttpProcessor
 * against a mock backend that introduces a configurable per-request work delay.
 *
 * The ActorSystem dispatcher is capped at exactly `threads` threads so the test
 * can directly measure whether the async proxy beats the blocking-thread estimate.
 *
 * Scenarios tested:
 *   threads : 1 / 4 / 8
 *   clients : 10 / 100 / 1000
 *   delayMs : 0 / 100 / 1000
 *
 * Assertions:
 *   1. Correctness  – all requests return HTTP 200, backend hit exactly `clients` times
 *   2. Throughput   – elapsed < clients * delayMs / threads  (blocking-thread lower bound)
 *                     (only checked when the gap is meaningful: blockingEstimate > 500 ms)
 *   3. Concurrency  – peakConcurrentBackendCalls > threads
 *                     (only checked when clients > threads and delayMs > 0)
 */
class ProxyThroughputPerfSpec extends AnyWordSpec with Matchers with ScalaFutures {

  // Generous patience: 1000 ms delay × 1000 clients async ≈ 3–5 s; give plenty of headroom.
  implicit val patience: PatienceConfig = PatienceConfig(120.seconds, 100.millis)

  private case class Scenario(threads: Int, clients: Int, delayMs: Long)

  // Override defaults via environment variables (comma-separated lists):
  //   PERF_THREADS=1,4,8  PERF_CLIENTS=10,100,1000  PERF_DELAYS=0,100,1000
  private def envInts(key: String, default: Seq[Int]): Seq[Int] =
    sys.env.get(key).map(_.split(',').map(_.trim.toInt).toSeq).getOrElse(default)

  private def envLongs(key: String, default: Seq[Long]): Seq[Long] =
    sys.env.get(key).map(_.split(',').map(_.trim.toLong).toSeq).getOrElse(default)

  private val threadCounts: Seq[Int]  = envInts("PERF_THREADS", Seq(1, 4, 8))
  private val clientCounts: Seq[Int]  = envInts("PERF_CLIENTS", Seq(10, 100, 1000))
  private val delayMsValues: Seq[Long] = envLongs("PERF_DELAYS", Seq(0L, 100L, 1000L))

  private val scenarios: Seq[Scenario] = for {
    t <- threadCounts
    c <- clientCounts
    d <- delayMsValues
  } yield Scenario(t, c, d)

  // ── test registration ───────────────────────────────────────────────────────

  "Proxy throughput" should {
    for (s <- scenarios) {
      s"${s.threads} threads / ${s.clients} clients / ${s.delayMs}ms backend work" in {
        runScenario(s)
      }
    }
  }

  // ── scenario runner ──────────────────────────────────────────────────────────

  private def runScenario(s: Scenario): Unit = {
    // max-connections and max-open-requests (power-of-2) sized to the client count
    val maxConn = math.max(s.clients + 16, 64)
    val maxOpen = { var p = 1; while (p < maxConn * 2) p <<= 1; p }

    val cfg = ConfigFactory.parseString(
      s"""
      akka {
        actor.default-dispatcher {
          type     = Dispatcher
          executor = "thread-pool-executor"
          thread-pool-executor.fixed-pool-size = ${s.threads}
          throughput = 1
        }
        http.host-connection-pool {
          max-connections   = $maxConn
          max-open-requests = $maxOpen
        }
      }
      """
    ).withFallback(ConfigFactory.load())

    implicit val system: ActorSystem             = ActorSystem(s"perf-${s.threads}t-${s.clients}c-${s.delayMs}d", cfg)
    implicit val ec: ExecutionContext            = system.dispatcher
    implicit val mat: Materializer               = Materializer(system)
    implicit val scheduler: akka.actor.Scheduler = system.scheduler

    try {

      // ── mock backend ─────────────────────────────────────────────────────────
      val hits           = new AtomicInteger(0)
      val activeCalls    = new AtomicInteger(0)
      val peakConcurrent = new AtomicInteger(0)

      val backendRoute = post {
        pathEndOrSingleSlash {
          entity(as[String]) { _ =>
            val n    = hits.incrementAndGet()
            val live = activeCalls.incrementAndGet()
            peakConcurrent.updateAndGet(prev => math.max(prev, live))

            // Non-blocking delay: thread is released immediately; response arrives after delayMs
            val delayed = akka.pattern.after(s.delayMs.millis, system.scheduler) {
              Future.successful {
                activeCalls.decrementAndGet()
                HttpResponse(entity = HttpEntity(
                  ContentTypes.`application/json`,
                  s"""{"result":"ok","n":$n}"""
                ))
              }
            }
            complete(delayed)
          }
        }
      }

      val backendBinding = Http().newServerAt("127.0.0.1", 0).bind(backendRoute).futureValue
      try {
        val backendAddr = backendBinding.localAddress
        val backendUrl  = s"http://${backendAddr.getHostString}:${backendAddr.getPort}/"

        // ── proxy pipeline: Header → Pool(rr) → Retry → HTTP ──────────────────
        val pipeline = ProcessorPipeline.fromSeq(
          Seq(
            new HeaderProcessor(
              addRequest  = Map("X-Proxy-Test" -> "perf"),
              onResponse  = true,
              addResponse = Map("X-Proxy-Served" -> "true")
            ),
            PoolProcessor.roundRobin(Seq(backendUrl)),
            new RetryProcessor(maxRetries = 2, delayMs = 20L),
            new HttpProcessor()
          ),
          "PerfTest"
        )

        val store        = new ProxyStorePipeline(pipeline, "perf")
        val proxyBinding = Http().newServerAt("127.0.0.1", 0)
          .bind(post {
            pathEndOrSingleSlash {
              entity(as[String]) { body =>
                onSuccess(store.proxy(HttpMethods.POST, "/", ByteString(body), Nil)) { sess =>
                  complete(HttpResponse(
                    status = sess.responseStatus,
                    entity = HttpEntity(ContentTypes.`application/json`,
                      sess.responseBody.map(_.utf8String).getOrElse(""))
                  ))
                }
              }
            }
          })
          .futureValue

        try {
          val proxyAddr = proxyBinding.localAddress
          val proxyUrl  = s"http://${proxyAddr.getHostString}:${proxyAddr.getPort}/"

          // ── fire all clients concurrently ────────────────────────────────────
          val startMs = System.currentTimeMillis()

          val futures = (1 to s.clients).map { _ =>
            Http()
              .singleRequest(HttpRequest(
                HttpMethods.POST,
                Uri(proxyUrl),
                entity = HttpEntity(ContentTypes.`application/json`,
                  """{"id":1,"method":"perf_test"}""")
              ))
              .flatMap(resp => Unmarshal(resp.entity).to[String].map(resp.status -> _))
          }

          val results   = Future.sequence(futures).futureValue
          val elapsedMs = System.currentTimeMillis() - startMs

          // ── 1. correctness ───────────────────────────────────────────────────
          results.size shouldBe s.clients
          results.foreach { case (status, _) => status shouldBe StatusCodes.OK }
          hits.get() shouldBe s.clients

          // ── 2. throughput (only when the async/blocking gap is meaningful) ────
          val blockingEstimateMs = if (s.delayMs > 0) s.clients.toLong * s.delayMs / s.threads else 0L

          if (blockingEstimateMs > 500) {
            withClue(s"elapsed ${elapsedMs}ms should beat blocking estimate ${blockingEstimateMs}ms: ") {
              elapsedMs should be < blockingEstimateMs
            }
          }

          // ── 3. concurrency (backend must serve >threads requests at once) ─────
          if (s.delayMs > 0 && s.clients > s.threads) {
            withClue(s"peak concurrent ${peakConcurrent.get()} should exceed thread count ${s.threads}: ") {
              peakConcurrent.get() should be > s.threads
            }
          }

          // ── report ───────────────────────────────────────────────────────────
          val speedup = if (blockingEstimateMs > 0) f"${blockingEstimateMs.toDouble / elapsedMs}%.1fx" else "n/a"
          info(
            s"threads=${s.threads}  clients=${s.clients}  delay=${s.delayMs}ms  " +
            s"elapsed=${elapsedMs}ms  blockingEst=${blockingEstimateMs}ms  " +
            s"speedup=$speedup  peak=${peakConcurrent.get()}"
          )

        } finally proxyBinding.unbind().futureValue
      } finally backendBinding.unbind().futureValue
    } finally Await.result(system.terminate(), 30.seconds)
  }
}
