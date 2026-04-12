package io.syspulse.ika.server

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer

import io.syspulse.ika.processor.{PipelineBuilder, ProcessorConfig, ProcessorPipeline}
import io.syspulse.ika.processor.impl.{
  HttpClientProcessor,
  LoadBalancerProcessor,
  RejectionProcessor,
  RetryProcessor,
  TimeoutProcessor
}
import io.syspulse.ika.processor.rpc3.CacheRpc3Processor
import io.syspulse.ika.store.{ProxyStore, ProxyStorePipeline}

/**
 * End-to-end tests: mock JSON-RPC backend (HTTP) → proxy pipeline ([[ProxyStorePipeline]]) →
 * proxy HTTP server → HTTP client ([[Http().singleRequest]]).
 *
 * Note: The built-in Web3 pipeline runs [[io.syspulse.ika.processor.impl.CacheRpc3Processor]]
 * before [[io.syspulse.ika.processor.impl.HttpClientProcessor]] in a single forward pass, so the
 * cache lookup runs on the request but the response is never fed back into the same processor in
 * that pass (response-phase caching would require a second pass or a store processor after HTTP).
 * Real cache **hits** over HTTP are tested below by wiring the same [[CacheRpc3Processor]]
 * instance twice (lookup before HTTP, store after HTTP), which the default [[PipelineBuilder]]
 * Web3 chain does not do in one pass.
 */
class ProxyPipelineHttpIntegrationSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("proxy-pipeline-http-it")
  implicit private val ec: ExecutionContext = system.dispatcher
  implicit private val mat: Materializer = Materializer(system)

  implicit private val patience: PatienceConfig =
    PatienceConfig(timeout = scaled(30.seconds), interval = scaled(100.millis))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private val ethBlockReq =
    """{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"""

  private def baseJsonResult(tag: String): String =
    s"""{"jsonrpc":"2.0","id":1,"result":"$tag"}"""

  /** Minimal proxy route: POST body → pipeline → JSON body (same role as [[ProxyRegistry]] + routes). */
  private def proxyRoute(store: ProxyStore): akka.http.scaladsl.server.Route =
    post {
      pathEndOrSingleSlash {
        entity(as[String]) { body =>
          onSuccess(store.rpc(body, Nil)) { pd =>
            complete(
              HttpResponse(
                status = StatusCodes.OK,
                entity = HttpEntity(ContentTypes.`application/json`, pd.body)
              )
            )
          }
        }
      }
    }

  private def pipelineStore(
      destinations: Seq[String],
      processorConfig: ProcessorConfig,
      cacheUri: String,
      poolStrategy: String = "sticky"
  ): ProxyStore = {
    val builder = PipelineBuilder(
      destinations = destinations,
      processorConfig = processorConfig,
      poolStrategy = poolStrategy,
      cacheUri = cacheUri
    )
    new ProxyStorePipeline(builder.build("web3"), "web3")
  }

  private def bind(route: akka.http.scaladsl.server.Route): Http.ServerBinding =
    Http().newServerAt("127.0.0.1", 0).bind(route).futureValue

  private def backendUrl(binding: Http.ServerBinding): String = {
    val a = binding.localAddress
    s"http://${a.getHostString}:${a.getPort}/"
  }

  private def postJson(url: String, body: String): String = {
    val req = HttpRequest(
      HttpMethods.POST,
      Uri(url),
      entity = HttpEntity(ContentTypes.`application/json`, body)
    )
    val resp = Http().singleRequest(req).futureValue
    resp.status shouldBe StatusCodes.OK
    Unmarshal(resp.entity).to[String].futureValue
  }

  "Proxy pipeline (HTTP → pipeline → HTTP client)" when {

    "using RPC3 cache (layer enabled)" should {
      "proxy JSON-RPC successfully with rpc3:// cache processor in the pipeline" in {
        val hits = new AtomicInteger(0)
        val backendRoute = post {
          pathEndOrSingleSlash {
            entity(as[String]) { _ =>
              hits.incrementAndGet()
              complete(HttpEntity(ContentTypes.`application/json`, baseJsonResult("0xabc")))
            }
          }
        }
        val backendBinding = bind(backendRoute)
        try {
          val store = pipelineStore(
            Seq(backendUrl(backendBinding)),
            ProcessorConfig.default.copy(timeout = 8000L, retryDelay = 50L),
            cacheUri = "rpc3://"
          )
          val px = bind(proxyRoute(store))
          try {
            val url = backendUrl(px)
            postJson(url, ethBlockReq) should include("0xabc")
            hits.get() shouldBe 1
          } finally px.unbind().futureValue
        } finally backendBinding.unbind().futureValue
      }
    }

    "with CacheRpc3 (same processor before and after HTTP)" should {
      "call the backend once for two identical requests (true cache hit on second)" in {
        implicit val scheduler: akka.actor.Scheduler = system.scheduler
        val hits = new AtomicInteger(0)
        val backendRoute = post {
          pathEndOrSingleSlash {
            entity(as[String]) { _ =>
              hits.incrementAndGet()
              complete(HttpEntity(ContentTypes.`application/json`, baseJsonResult("0xabc")))
            }
          }
        }
        val backendBinding = bind(backendRoute)
        try {
          val dest = backendUrl(backendBinding)
          val cache = CacheRpc3Processor.expire()
          val timeout = new TimeoutProcessor(8000L, Some(50L))
          val lb = LoadBalancerProcessor.sticky(Seq(dest))
          val http = HttpClientProcessor("")
          val retry = new RetryProcessor(http, maxRetries = 3, delayMs = 50L)
          val rej = RejectionProcessor.jsonRpc(httpStatusCode = 200)
          val pipeline = ProcessorPipeline.fromSeq(
            Seq(cache, timeout, lb, retry, cache, rej),
            "CacheRpc3"
          )
          val store = new ProxyStorePipeline(pipeline, "cache-rpc3")
          val px = bind(proxyRoute(store))
          try {
            val url = backendUrl(px)
            postJson(url, ethBlockReq) should include("0xabc")
            postJson(url, ethBlockReq) should include("0xabc")
            hits.get() shouldBe 1
          } finally px.unbind().futureValue
        } finally backendBinding.unbind().futureValue
      }
    }

    "using no cache (none://)" should {
      "call the backend for every request" in {
        val hits = new AtomicInteger(0)
        val backendRoute = post {
          pathEndOrSingleSlash {
            entity(as[String]) { _ =>
              hits.incrementAndGet()
              complete(HttpEntity(ContentTypes.`application/json`, baseJsonResult("0x1")))
            }
          }
        }
        val backendBinding = bind(backendRoute)
        try {
          val store = pipelineStore(
            Seq(backendUrl(backendBinding)),
            ProcessorConfig.default.copy(timeout = 8000L, retryDelay = 50L),
            cacheUri = "none://"
          )
          val px = bind(proxyRoute(store))
          try {
            val url = backendUrl(px)
            postJson(url, ethBlockReq)
            postJson(url, ethBlockReq)
            hits.get() shouldBe 2
          } finally px.unbind().futureValue
        } finally backendBinding.unbind().futureValue
      }
    }

    "using RetryProcessor" should {
      "return success after transient HTTP errors on the backend" in {
        val attempts = new AtomicInteger(0)
        val backendRoute = post {
          pathEndOrSingleSlash {
            entity(as[String]) { _ =>
              val n = attempts.incrementAndGet()
              if (n <= 2)
                complete(StatusCodes.ServiceUnavailable, "busy")
              else
                complete(HttpEntity(ContentTypes.`application/json`, baseJsonResult("0xok")))
            }
          }
        }
        val backendBinding = bind(backendRoute)
        try {
          val store = pipelineStore(
            Seq(backendUrl(backendBinding)),
            ProcessorConfig.default.copy(
              timeout = 8000L,
              retry = 4,
              retryDelay = 30L
            ),
            cacheUri = "none://"
          )
          val px = bind(proxyRoute(store))
          try {
            val body = postJson(backendUrl(px), ethBlockReq)
            body should include("0xok")
            assert(attempts.get() >= 3)
          } finally px.unbind().futureValue
        } finally backendBinding.unbind().futureValue
      }
    }

    "using TimeoutProcessor" should {
      "surface an error when the backend is slower than the configured timeout" in {
        val backendRoute = post {
          pathEndOrSingleSlash {
            entity(as[String]) { _ =>
              val delayed = akka.pattern.after(600.millis, system.scheduler)(
                Future.successful(
                  HttpResponse(
                    entity = HttpEntity(ContentTypes.`application/json`, baseJsonResult("late"))
                  )
                )
              )
              complete(delayed)
            }
          }
        }
        val backendBinding = bind(backendRoute)
        try {
          val store = pipelineStore(
            Seq(backendUrl(backendBinding)),
            ProcessorConfig.default.copy(
              timeout = 80L,
              retry = 0,
              retryDelay = 10L
            ),
            cacheUri = "none://"
          )
          val px = bind(proxyRoute(store))
          try {
            val req = HttpRequest(
              HttpMethods.POST,
              Uri(backendUrl(px)),
              entity = HttpEntity(ContentTypes.`application/json`, ethBlockReq)
            )
            val resp = Http().singleRequest(req).futureValue
            resp.status shouldBe StatusCodes.OK
            val body = Unmarshal(resp.entity).to[String].futureValue
            body should include("error")
          } finally px.unbind().futureValue
        } finally backendBinding.unbind().futureValue
      }
    }

    "using LoadBalancerProcessor (round-robin)" should {
      "spread requests across two backends" in {
        val b1hits = new AtomicInteger(0)
        val b2hits = new AtomicInteger(0)
        val r1 = post {
          pathEndOrSingleSlash {
            entity(as[String]) { _ =>
              b1hits.incrementAndGet()
              complete(HttpEntity(ContentTypes.`application/json`, baseJsonResult("A")))
            }
          }
        }
        val r2 = post {
          pathEndOrSingleSlash {
            entity(as[String]) { _ =>
              b2hits.incrementAndGet()
              complete(HttpEntity(ContentTypes.`application/json`, baseJsonResult("B")))
            }
          }
        }
        val be1 = bind(r1)
        val be2 = bind(r2)
        try {
          val d1 = backendUrl(be1)
          val d2 = backendUrl(be2)
          val store = pipelineStore(
            Seq(d1, d2),
            ProcessorConfig.default.copy(timeout = 8000L, retryDelay = 50L),
            cacheUri = "none://",
            poolStrategy = "lb"
          )
          val px = bind(proxyRoute(store))
          try {
            val url = backendUrl(px)
            val out1 = postJson(url, ethBlockReq)
            val out2 = postJson(url, ethBlockReq)
            out1 should not equal out2
            out1 should (include("\"result\":\"A\"") or include("A"))
            out2 should (include("\"result\":\"B\"") or include("B"))
            b1hits.get() shouldBe 1
            b2hits.get() shouldBe 1
          } finally px.unbind().futureValue
        } finally {
          be1.unbind().futureValue
          be2.unbind().futureValue
        }
      }
    }
  }
}
