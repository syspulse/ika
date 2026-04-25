package io.syspulse.ika.processor.impl

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.ActorSystem

import io.syspulse.ika.processor.Session
import akka.util.ByteString

class PoolProcessorDestinationsSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("pool-destinations-spec")

  override protected def afterAll(): Unit = {
    actorSystem.terminate()
    super.afterAll()
  }

  "PoolProcessor destinations" should {
    "accept name=uri destinations from config and set destination to uri only" in {
      val cfg = ConfigFactory.parseString(
        """
          |strategy = "sticky"
          |destinations = [
          |  "host1=http://localhost:8080",
          |  "host2=http://localhost:8081"
          |]
          |""".stripMargin
      )

      val p = PoolProcessor.fromConfig("pool_1", cfg).head.asInstanceOf[PoolProcessor]

      val s0 = Session(requestBody = ByteString("x"))
      val s1 = Await.result(p.process(s0), 2.seconds)

      // Must be URI only (no "host1:" prefix)
      s1.getData[String]("destination") should (be(Some("http://localhost:8080")) or be(Some("http://localhost:8081")))
    }
  }
}

