package io.syspulse.ika.processor.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class ProcessorPipelineBuilderFromFileSpec extends AnyWordSpec with Matchers {

  implicit private val system: ActorSystem = ActorSystem("pipeline-from-file-spec")
  implicit private val ec: ExecutionContext = system.dispatcher

  private def parseConfFileIgnoringIncludes(path: String) = {
    val lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8).asScala.toSeq
    val body = lines
      .filterNot(_.trim.startsWith("include "))
      .mkString("\n")
    ConfigFactory.parseString(body)
  }

  "ProcessorPipelineBuilder.fromConfig" should {
    "build pipeline from conf/application-ika.conf example" in {
      val cfg = parseConfFileIgnoringIncludes("conf/application-ika.conf")
      cfg.hasPath("processors") shouldBe true

      val pipeline = ProcessorPipelineBuilder.fromConfig(cfg)
      val s = pipeline.toString

      // application-ika.conf processors: throttle_1, pool_1, http_1
      s should include("Throttle")
      s should include("Pool")
      s should include("Http")
    }

    "build pipeline from conf/application-ika.conf profile.rpc3" in {
      val cfg = parseConfFileIgnoringIncludes("conf/application-ika.conf")
      cfg.hasPath("profiles.rpc3.processors") shouldBe true

      val pipeline = ProcessorPipelineBuilder.fromConfig(cfg, "rpc3", None)
      val s = pipeline.toString

      s should include("Pool")
      s should include("Rpc3")
      s should include("Http")
    }

    "build pipeline from conf/application-ika.conf profile.proxy" in {
      val cfg = parseConfFileIgnoringIncludes("conf/application-ika.conf")
      cfg.hasPath("profiles.proxy.processors") shouldBe true

      val pipeline = ProcessorPipelineBuilder.fromConfig(cfg, "proxy", None)
      val s = pipeline.toString

      s should include("Http")
    }

    "build pipeline generically via fromProfile(appCfg, name, ...) for custom profiles" in {
      val cfg = parseConfFileIgnoringIncludes("conf/application-ika.conf")
      val pipeline = ProcessorPipelineBuilder.fromProfile(cfg, "http-pool", None)
      val s = pipeline.toString

      // profile.http-pool = "pool_1, http_1"
      s should include("Pool")
      s should include("Http")
    }

    "build pipelines for all conf/application*.conf files that define processors" in {
      val dir = Paths.get("conf")
      val files = Files.list(dir).iterator().asScala
        .map(_.toString)
        .filter(p => p.endsWith(".conf") && Paths.get(p).getFileName.toString.startsWith("application"))
        .toSeq

      files.nonEmpty shouldBe true

      files.foreach { f =>
        val cfg = parseConfFileIgnoringIncludes(f)
        if (cfg.hasPath("processors")) {
          val pipeline = ProcessorPipelineBuilder.fromConfig(cfg)
          pipeline.processors.nonEmpty shouldBe true
        }
      }
    }
  }
}

