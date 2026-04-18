package io.syspulse.ika.processor.uri

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class PoolURISpec extends AnyWordSpec with Matchers {

  private val fb = Seq("http://fallback:8545")

  "PoolURI destinations" should {

    "use fallback when uri has no list after ://" in {
      PoolURI("lb://").destinations(fb) shouldBe fb
      PoolURI("sticky://").destinations(Seq.empty) shouldBe Seq.empty
    }

    "parse comma-separated http URLs" in {
      val d = PoolURI("lb://http://127.0.0.1:1,http://127.0.0.1:2").destinations(fb)
      d shouldBe Seq("http://127.0.0.1:1", "http://127.0.0.1:2")
    }

    "parse pipe-separated URLs (ignores fallback)" in {
      PoolURI("roundrobin://http://a:8545|http://b:8545|http://c:8545").destinations(fb) shouldBe Seq(
        "http://a:8545",
        "http://b:8545",
        "http://c:8545"
      )
    }

    "prefer embedded list over fallback" in {
      PoolURI("sticky://http://only/").destinations(Seq("http://ignored", "http://ignored2")) shouldBe Seq("http://only/")
    }

    "trim whitespace around entries" in {
      PoolURI("lb:// http://x:1 | http://y:2 ").destinations(fb) shouldBe Seq("http://x:1", "http://y:2")
    }

    "support query params (?&)" in {
      val p = PoolURI("lb://http://a:1|http://b:2?foo=bar&x=1")
      p.strategy shouldBe "lb"
      p.destinations(fb) shouldBe Seq("http://a:1", "http://b:2")
      p.ops shouldBe Map("foo" -> "bar", "x" -> "1")
    }

    "support pool tag prefix syntax (tag=...)" in {
      val p = PoolURI("lb://tag1=http://127.0.0.1:1,http://127.0.0.1:2")
      p.destinations(fb) shouldBe Seq("tag1:http://127.0.0.1:1", "tag1:http://127.0.0.1:2")
    }
  }
}
