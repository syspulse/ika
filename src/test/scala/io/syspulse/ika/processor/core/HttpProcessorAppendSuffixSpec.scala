package io.syspulse.ika.processor.core

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.syspulse.ika.processor.core.HttpProcessor

class HttpProcessorAppendSuffixSpec extends AnyWordSpec with Matchers {

  "HttpProcessor.appendSuffix" should {
    "append plain path suffix" in {
      HttpProcessor.appendSuffix("http://localhost:8300", "/test/1").get shouldBe "http://localhost:8300/test/1"
      HttpProcessor.appendSuffix("http://localhost:8300/", "/test/1").get shouldBe "http://localhost:8300/test/1"
    }

    "append suffix preserving base path" in {
      HttpProcessor.appendSuffix("http://localhost:8300/base", "/test/1").get shouldBe "http://localhost:8300/base/test/1"
      HttpProcessor.appendSuffix("http://localhost:8300/base/", "/test/1").get shouldBe "http://localhost:8300/base/test/1"
    }

    "append suffix query string" in {
      HttpProcessor.appendSuffix("http://localhost:8300", "/test/1?x=1").get shouldBe "http://localhost:8300/test/1?x=1"
      HttpProcessor.appendSuffix("http://localhost:8300/base/", "/test/1?x=1&y=2").get shouldBe "http://localhost:8300/base/test/1?x=1&y=2"
    }

    "ignore empty or root suffix" in {
      HttpProcessor.appendSuffix("http://localhost:8300", "").get shouldBe "http://localhost:8300"
      HttpProcessor.appendSuffix("http://localhost:8300", "/").get shouldBe "http://localhost:8300"
    }
  }
}

