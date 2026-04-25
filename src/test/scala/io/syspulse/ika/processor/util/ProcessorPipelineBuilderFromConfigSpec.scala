package io.syspulse.ika.processor.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpHeader

import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext

import io.syspulse.ika.processor.Session
import io.syspulse.ika.processor.util.ProcessorPipelineBuilder
import akka.util.ByteString

class ProcessorPipelineBuilderFromConfigSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit private val system: ActorSystem = ActorSystem("pipeline-from-config-spec")
  implicit private val ec: ExecutionContext = system.dispatcher
  implicit private val patience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  "ProcessorPipelineBuilder.fromConfig" should {
    "build a pipeline matching application-ika.conf style" in {
      val cfg = ConfigFactory.parseString("""
        processors = "throttle_1, pool_1"

        throttle_1 {
          type = "throttle://"
          throttle = 1
        }

        pool_1 {
          type = "pool://"
          strategy = "lb"
          destinations = [
            "host1=http://localhost:8080",
            "host2=http://localhost:8081"
          ]
        }
      """)

      val pipeline = ProcessorPipelineBuilder.fromConfig(cfg)
      pipeline.toString should include("Throttle")
      pipeline.toString should include("Pool")
    }

    "build http processor and apply headers/method" in {
      val cfg = ConfigFactory.parseString("""
        processors = "http_1"
        http_1 {
          type = "http://"
          method = "GET"
          headers = {
            "Content-Type" = "application/json"
            "X-Test" = "1"
          }
        }
      """)

      val pipeline = ProcessorPipelineBuilder.fromConfig(cfg)
      pipeline.toString should include("Http")

      val s0 = Session(requestBody = ByteString("""{"a":1}"""), requestHeaders = Nil)
      whenReady(pipeline.process(s0)) { s1 =>
        val hs: Seq[HttpHeader] = s1.requestHeaders
        hs.exists(h => h.lowercaseName() == "x-test" && h.value() == "1") shouldBe true
      }
    }
  }
}

