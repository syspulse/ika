package io.syspulse.ika.processor.core

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.http.scaladsl.model.headers.RawHeader

import io.syspulse.ika.processor.Session
import io.syspulse.ika.processor.core.MetaProcessor

class MetaProcessorSpec extends AnyWordSpec with Matchers {
  implicit val ec: ExecutionContext = ExecutionContext.global

  "MetaProcessor" should {
    "extract meta keys and key/value pairs and strip meta headers" in {
      implicit val as: ActorSystem = ActorSystem("meta-spec")
      try {
        val p = new MetaProcessor(Seq("x-meta"))(ec)
        val s0 = Session(requestBody = ByteString("{}"))
          .addRequestHeader(RawHeader("x-meta", "tid = 1024, tx=0x123, flag"))
          .addRequestHeader(RawHeader("other", "1"))

        val s1 = Await.result(p.processRequest(s0), 3.seconds)

        s1.requestHeaders.exists(_.lowercaseName() == "x-meta") shouldBe false
        s1.requestHeaders.exists(_.lowercaseName() == "other") shouldBe true

        val meta = s1.getData[Map[String, String]]("meta").getOrElse(Map.empty)
        meta("tid") shouldBe "1024"
        meta("tx") shouldBe "0x123"
        meta("flag") shouldBe ""
      } finally Await.result(as.terminate(), 5.seconds)
    }

    "round-trip a real UUID value" in {
      implicit val as: ActorSystem = ActorSystem("meta-uuid-spec")
      try {
        val p = new MetaProcessor(Seq("x-meta"))(ec)
        val uuid = java.util.UUID.randomUUID().toString
        val s0 = Session(requestBody = ByteString("{}"))
          .addRequestHeader(RawHeader("x-meta", s"uuid=$uuid"))

        val s1 = Await.result(p.processRequest(s0), 3.seconds)
        val meta = s1.getData[Map[String, String]]("meta").getOrElse(Map.empty)
        meta.contains("uuid") shouldBe true
        meta("uuid") shouldBe uuid
      } finally Await.result(as.terminate(), 5.seconds)
    }
  }
}

