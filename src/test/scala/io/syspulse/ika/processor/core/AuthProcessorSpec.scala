package io.syspulse.ika.processor.core

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.util.ByteString

import io.syspulse.ika.processor.Session
import io.syspulse.ika.processor.core.{AuthProcessor, AuthStrategy}

class AuthProcessorSpec extends AnyWordSpec with Matchers {
  implicit val ec: ExecutionContext = ExecutionContext.global

  "AuthProcessor" should {
    "verify Authorization Bearer header and strip it" in {
      implicit val as: ActorSystem = ActorSystem("auth-bearer-spec")
      try {
        val strategyCfg = ConfigFactory.parseString(
          """
            strategy = "bearer"
            bearer {
              secret = "sk-test"
            }
          """
        ).resolve()
        val p = new AuthProcessor(strategy = AuthStrategy.fromConfig(strategyCfg, as.settings.config))(ec)
        val s0 = Session(requestBody = ByteString("{}"))
          .addRequestHeader(akka.http.scaladsl.model.headers.RawHeader("Authorization", "Bearer sk-test"))
        val s1 = Await.result(p.processRequest(s0), 3.seconds)
        s1.isRejected shouldBe false
        s1.requestHeaders.exists(_.lowercaseName() == "authorization") shouldBe false
      } finally Await.result(as.terminate(), 5.seconds)
    }

    "verify custom 'header' header and strip it" in {
      implicit val as: ActorSystem = ActorSystem("auth-header-spec")
      try {
        val strategyCfg = ConfigFactory.parseString(
          """
            strategy = "header"
            header {
              secret = "abc"
              name = "x-api-key"
            }
          """
        ).resolve()
        val p = new AuthProcessor(strategy = AuthStrategy.fromConfig(strategyCfg, as.settings.config))(ec)
        val s0 = Session(requestBody = ByteString("{}"))
          .addRequestHeader(akka.http.scaladsl.model.headers.RawHeader("x-api-key", "abc"))
        val s1 = Await.result(p.processRequest(s0), 3.seconds)
        s1.isRejected shouldBe false
        s1.requestHeaders.exists(_.lowercaseName() == "x-api-key") shouldBe false
      } finally Await.result(as.terminate(), 5.seconds)
    }

    "read bearer secret from bearer subtree" in {
      implicit val as: ActorSystem = ActorSystem("auth-bearer-subtree-spec")
      try {
        val cfg = ConfigFactory.parseString(
          """
            type = "auth://"
            strategy = "bearer"
            bearer {
              secret = "super-secret"
            }
          """
        ).resolve()

        val p = AuthProcessor.fromConfig("auth_1", cfg)(ec, as).head.asInstanceOf[AuthProcessor]
        val s0 = Session(requestBody = ByteString("{}"))
          .addRequestHeader(akka.http.scaladsl.model.headers.RawHeader("Authorization", "Bearer super-secret"))
        val s1 = Await.result(p.processRequest(s0), 3.seconds)
        s1.isRejected shouldBe false
        s1.requestHeaders.exists(_.lowercaseName() == "authorization") shouldBe false
      } finally Await.result(as.terminate(), 5.seconds)
    }

    "reject when secret does not match" in {
      implicit val as: ActorSystem = ActorSystem("auth-reject-spec")
      try {
        val strategyCfg = ConfigFactory.parseString(
          """
            strategy = "bearer"
            bearer {
              secret = "sk-test"
            }
          """
        ).resolve()
        val p = new AuthProcessor(strategy = AuthStrategy.fromConfig(strategyCfg, as.settings.config))(ec)
        val s0 = Session(requestBody = ByteString("{}"))
          .addRequestHeader(akka.http.scaladsl.model.headers.RawHeader("Authorization", "Bearer wrong"))
        val s1 = Await.result(p.processRequest(s0), 3.seconds)
        s1.isRejected shouldBe true
        s1.rejection.map(_.code) shouldBe Some(401)
      } finally Await.result(as.terminate(), 5.seconds)
    }
  }
}

