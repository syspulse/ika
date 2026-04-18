package io.syspulse.ika.processor.uri

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class CacheURISpec extends AnyWordSpec with Matchers {

  private val DefTtl = 30000L
  private val DefGc = 10000L
  private val DefTtlLatest = 12000L

  "CacheURI" should {

    "parse none://" in {
      val c = CacheURI("none://")
      c.kind shouldBe "none"
      c.ttl shouldBe DefTtl
      c.ttlLatest shouldBe DefTtlLatest
      c.gcFreq shouldBe DefGc
    }

    "parse expire:// with defaults" in {
      val c = CacheURI("expire://")
      c.kind shouldBe "expire"
      c.ttl shouldBe DefTtl
      c.ttlLatest shouldBe DefTtlLatest
      c.gcFreq shouldBe DefGc
    }

    "parse expire:// with ttl only" in {
      val c = CacheURI("expire://45000")
      c.kind shouldBe "expire"
      c.ttl shouldBe 45000L
      c.ttlLatest shouldBe DefTtlLatest
      c.gcFreq shouldBe DefGc
    }

    "parse expire:// with ttl and gcFreq" in {
      val c = CacheURI("expire://45000,5000")
      c.kind shouldBe "expire"
      c.ttl shouldBe 45000L
      c.ttlLatest shouldBe DefTtlLatest
      c.gcFreq shouldBe 5000L
    }

    "trim spaces around comma-separated params" in {
      val c = CacheURI("expire://45000 , 5000 ")
      c.ttl shouldBe 45000L
      c.gcFreq shouldBe 5000L
    }

    "parse rpc3:// with defaults" in {
      val c = CacheURI("rpc3://")
      c.kind shouldBe "rpc3"
      c.ttl shouldBe DefTtl
      c.ttlLatest shouldBe DefTtlLatest
      c.gcFreq shouldBe DefGc
    }

    "parse rpc3:// with ttl only" in {
      val c = CacheURI("rpc3://60000")
      c.kind shouldBe "rpc3"
      c.ttl shouldBe 60000L
      c.ttlLatest shouldBe DefTtlLatest
      c.gcFreq shouldBe DefGc
    }

    "parse rpc3:// with ttl and ttlLatest" in {
      val c = CacheURI("rpc3://60000,15000")
      c.kind shouldBe "rpc3"
      c.ttl shouldBe 60000L
      c.ttlLatest shouldBe 15000L
      c.gcFreq shouldBe DefGc
    }

    "parse rpc3:// with ttl, ttlLatest, and gcFreq" in {
      val c = CacheURI("rpc3://60000,15000,7000")
      c.kind shouldBe "rpc3"
      c.ttl shouldBe 60000L
      c.ttlLatest shouldBe 15000L
      c.gcFreq shouldBe 7000L
    }

    "trim surrounding whitespace on uri" in {
      val c = CacheURI("  rpc3://5000  ")
      c.kind shouldBe "rpc3"
      c.ttl shouldBe 5000L
    }

    "default to expire when there is no :// separator" in {
      val c = CacheURI("not-a-uri")
      c.kind shouldBe "expire"
      c.ttl shouldBe DefTtl
      c.gcFreq shouldBe DefGc
    }

    "default to expire for unknown scheme" in {
      val c = CacheURI("unknown://stuff")
      c.kind shouldBe "expire"
      c.ttl shouldBe DefTtl
      c.gcFreq shouldBe DefGc
    }

    "parse expire:// with query params (?&)" in {
      val c = CacheURI("expire://?ttl=45000&gc=5000")
      c.kind shouldBe "expire"
      c.ttl shouldBe 45000L
      c.gcFreq shouldBe 5000L
      c.ops("ttl") shouldBe "45000"
      c.ops("gc") shouldBe "5000"
    }

    "parse rpc3:// with query params (?&)" in {
      val c = CacheURI("rpc3://?ttl=60000&latest=15000&gc=7000")
      c.kind shouldBe "rpc3"
      c.ttl shouldBe 60000L
      c.ttlLatest shouldBe 15000L
      c.gcFreq shouldBe 7000L
    }
  }
}
