package io.syspulse.ika.processor.rpc3

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.io.Source
import scala.jdk.CollectionConverters._

import io.syspulse.ika.processor.{ProcessorPipeline, Session}
import io.syspulse.ika.processor.ResponseSource
import io.syspulse.ika.processor.util.ProcessorPipelineBuilder

/**
 * Integration test: build the real `profiles.solana` pipeline from `conf/application.conf`
 * (rpc_solana + pool_solana + retry_3 + http_client) and verify getBlock is stored then served from cache.
 */
class SolanaPipelineCacheSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {

  private var system: ActorSystem = _
  implicit private def actorSystem: ActorSystem = system
  implicit private def ec: ExecutionContext = system.dispatcher

  implicit private val patience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  override def beforeAll(): Unit = {
    // build.sbt sets config.file=conf/application.conf; resolve required substitutions before Akka loads it
    System.setProperty("SOL_RPC_URL", "http://127.0.0.1:9/")
    System.setProperty("ETH_RPC_URL", "http://127.0.0.1:9/")
    system = ActorSystem("solana-pipeline-cache-spec")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (system != null) system.terminate()
    super.afterAll()
  }

  private val testDir = Paths.get(System.getProperty("user.dir"), "test", "rpc3", "solana")

  private def loadFixture(name: String): String =
    Source.fromFile(testDir.resolve(name).toFile, "UTF-8").mkString.trim

  /** Same shape as conf/application.conf but without unresolved `include` directives. */
  private def applicationConfWithoutIncludes() = {
    val lines = Files.readAllLines(Paths.get("conf/application.conf"), StandardCharsets.UTF_8).asScala.toSeq
    val body = lines.filterNot(_.trim.startsWith("include ")).mkString("\n")
    ConfigFactory.parseString(body)
  }

  private def bindBackend(responseBody: String, hits: AtomicInteger): Http.ServerBinding = {
    val route = post {
      pathEndOrSingleSlash {
        entity(as[String]) { _ =>
          hits.incrementAndGet()
          complete(HttpEntity(ContentTypes.`application/json`, responseBody))
        }
      }
    }
    Http().newServerAt("127.0.0.1", 0).bind(route).futureValue
  }

  private def backendUrl(binding: Http.ServerBinding): String = {
    val a = binding.localAddress
    s"http://${a.getHostString}:${a.getPort}/"
  }

  private def solanaPipeline(backendUrl: String): ProcessorPipeline = {
    val cfg = ConfigFactory
      .parseString(s"""
        SOL_RPC_URL = "$backendUrl"
        ETH_RPC_URL = "http://127.0.0.1:9/"
      """)
      .withFallback(applicationConfWithoutIncludes())
      .resolve()

    cfg.hasPath("profiles.solana.processors") shouldBe true
    cfg.getString("profiles.solana.processors") shouldBe "rpc_solana, pool_solana, retry_3, http_client"

    ProcessorPipelineBuilder.fromProfile(cfg, "solana", None)
  }

  "solana profile (application.conf)" should {

    "cache getBlock without commitment — REMOTE then CACHE, backend called once" in {
      val hits = new AtomicInteger(0)
      val rsp = loadFixture("RSP_getBlock.json")
      val req =
        """{"jsonrpc":"2.0","method":"getBlock","params":[123456789,{"encoding":"jsonParsed","maxSupportedTransactionVersion":0,"transactionDetails":"full","rewards":true}],"id":123456789}"""

      val backend = bindBackend(rsp, hits)
      try {
        val pipeline = solanaPipeline(backendUrl(backend))
        pipeline.toString should include("SolanaRpc3")

        val r1 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
        r1.responseSource shouldBe ResponseSource.REMOTE
        r1.responseBody.map(_.utf8String).getOrElse("") should include("parentSlot")
        hits.get() shouldBe 1

        eventually(timeout(3.seconds), interval(25.millis)) {
          val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
          r2.responseSource shouldBe ResponseSource.CACHE
          r2.getData[Boolean]("cacheHit") shouldBe Some(true)
          r2.responseBody.map(_.utf8String).getOrElse("") should include("parentSlot")
          hits.get() shouldBe 1
        }
      } finally backend.unbind().futureValue
    }

    "cache batch getBlock — LOOKUP per item, second batch hits cache" in {
      val hits = new AtomicInteger(0)
      val rsp = loadFixture("RSP_Batch_getBlock.json")
      val req = loadFixture("REQ_Batch_getBlock.json")

      val backend = bindBackend(rsp, hits)
      try {
        val pipeline = solanaPipeline(backendUrl(backend))

        pipeline.process(Session(requestBody = ByteString(req))).futureValue.responseSource shouldBe ResponseSource.REMOTE
        hits.get() shouldBe 1

        eventually(timeout(3.seconds), interval(25.millis)) {
          val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
          r2.responseSource shouldBe ResponseSource.CACHE
          hits.get() shouldBe 1
        }
      } finally backend.unbind().futureValue
    }

    "cache getBlock with commitment — REMOTE then CACHE, backend called once" in {
      val hits = new AtomicInteger(0)
      val req = loadFixture("REQ_getBlock_finalized.json")
      val rsp = loadFixture("RSP_getBlock.json")

      val backend = bindBackend(rsp, hits)
      try {
        val pipeline = solanaPipeline(backendUrl(backend))

        pipeline.process(Session(requestBody = ByteString(req))).futureValue.responseSource shouldBe ResponseSource.REMOTE
        hits.get() shouldBe 1

        eventually(timeout(3.seconds), interval(25.millis)) {
          val r2 = pipeline.process(Session(requestBody = ByteString(req))).futureValue
          r2.responseSource shouldBe ResponseSource.CACHE
          hits.get() shouldBe 1
        }
      } finally backend.unbind().futureValue
    }
  }
}
