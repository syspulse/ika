package io.syspulse.ika.processor.impl

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.util.ByteString

import io.syspulse.ika.processor.{ProcessorPipeline, Session}
import io.syspulse.ika.store.ProxyStorePipeline

import org.scalatest.concurrent.ScalaFutures

class ProxyProcessorSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val patience: PatienceConfig = PatienceConfig(10.seconds, 50.millis)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  private def session(headers: Seq[HttpHeader] = Nil): Session =
    Session(requestBody = ByteString.empty, requestHeaders = headers)

  private def run(p: ProxyProcessor, s: Session): Session =
    p.processRequest(s).futureValue

  // ── hop-by-hop stripping ───────────────────────────────────────────────────

  "ProxyProcessor" should {

    "strip all standard RFC-7230 hop-by-hop headers from the request" in {
      val hopByHop = Seq(
        RawHeader("Connection",           "keep-alive"),
        RawHeader("Keep-Alive",           "timeout=5"),
        RawHeader("Proxy-Authenticate",   "Basic realm=proxy"),
        RawHeader("Proxy-Authorization",  "Basic dXNlcjpwYXNz"),
        RawHeader("TE",                   "trailers"),
        RawHeader("Trailer",              "Max-Forwards"),
        RawHeader("Transfer-Encoding",    "chunked"),
        RawHeader("Upgrade",              "h2c"),
        RawHeader("Proxy-Connection",     "keep-alive"),
        RawHeader("X-Safe",               "should-survive")
      )

      val result = run(new ProxyProcessor(addVia = false), session(hopByHop))
      val names  = result.requestHeaders.map(_.name.toLowerCase)

      names should not contain "connection"
      names should not contain "keep-alive"
      names should not contain "proxy-authenticate"
      names should not contain "proxy-authorization"
      names should not contain "te"
      names should not contain "trailer"
      names should not contain "transfer-encoding"
      names should not contain "upgrade"
      names should not contain "proxy-connection"
      names should contain ("x-safe")
    }

    "strip dynamic hop-by-hop headers listed in the Connection header value" in {
      // Connection: close, X-Custom-Internal names X-Custom-Internal as hop-by-hop
      val headers = Seq(
        RawHeader("Connection",       "close, X-Custom-Internal"),
        RawHeader("X-Custom-Internal","secret"),
        RawHeader("X-Public",         "visible")
      )

      val result = run(new ProxyProcessor(addVia = false), session(headers))
      val names  = result.requestHeaders.map(_.name.toLowerCase)

      names should not contain "connection"
      names should not contain "x-custom-internal"
      names should contain ("x-public")
    }

    "strip extra request headers configured via extraStripRequest" in {
      val headers = Seq(
        RawHeader("X-Internal-Token", "secret"),
        RawHeader("X-Public",         "visible")
      )

      val result = run(
        new ProxyProcessor(addVia = false, extraStripRequest = Set("X-Internal-Token")),
        session(headers)
      )
      val names = result.requestHeaders.map(_.name.toLowerCase)

      names should not contain "x-internal-token"
      names should contain ("x-public")
    }

    "append Via header to a request with no existing Via" in {
      val result = run(new ProxyProcessor(addVia = true), session())
      val via = result.requestHeaders.find(_.name.equalsIgnoreCase("Via"))
      via should be (defined)
      via.get.value should include ("ika-proxy")
    }

    "append to an existing Via header rather than replace it" in {
      val headers = Seq(RawHeader("Via", "1.1 upstream-proxy"))
      val result  = run(new ProxyProcessor(addVia = true), session(headers))
      val via     = result.requestHeaders.find(_.name.equalsIgnoreCase("Via"))
      via should be (defined)
      via.get.value should include ("upstream-proxy")
      via.get.value should include ("ika-proxy")
    }

    "not add Via when addVia = false" in {
      val result = run(new ProxyProcessor(addVia = false), session())
      result.requestHeaders.find(_.name.equalsIgnoreCase("Via")) should be (None)
    }

    "use a custom viaValue when configured" in {
      val result = run(new ProxyProcessor(addVia = true, viaValue = "1.1 my-company-proxy"), session())
      val via = result.requestHeaders.find(_.name.equalsIgnoreCase("Via"))
      via should be (defined)
      via.get.value shouldBe "1.1 my-company-proxy"
    }

    "chain custom viaValue with existing Via header" in {
      val headers = Seq(RawHeader("Via", "1.0 old-proxy"))
      val result  = run(new ProxyProcessor(addVia = true, viaValue = "1.1 new-proxy"), session(headers))
      val via     = result.requestHeaders.find(_.name.equalsIgnoreCase("Via"))
      via should be (defined)
      via.get.value shouldBe "1.0 old-proxy, 1.1 new-proxy"
    }

    // ── destination detection ─────────────────────────────────────────────────

    "set destination from Host header when no destination is present" in {
      val headers = Seq(RawHeader("Host", "api.example.com"))
      val result  = run(new ProxyProcessor(scheme = "https", addVia = false), session(headers))
      result.getData[String]("destination") shouldBe Some("https://api.example.com")
    }

    "not overwrite destination when one is already set on the session" in {
      val headers = Seq(RawHeader("Host", "api.example.com"))
      val s = session(headers).putData("destination", "http://pool.internal")
      val result = run(new ProxyProcessor(addVia = false), s)
      result.getData[String]("destination") shouldBe Some("http://pool.internal")
    }

    "use http scheme by default when Host has no scheme" in {
      val headers = Seq(RawHeader("Host", "backend.local:8080"))
      val result  = run(new ProxyProcessor(addVia = false), session(headers))
      result.getData[String]("destination") shouldBe Some("http://backend.local:8080")
    }

    "leave no destination when Host header is absent" in {
      val result = run(new ProxyProcessor(addVia = false), session())
      result.getData[String]("destination") shouldBe None
    }

    // ── response stripping ────────────────────────────────────────────────────

    "strip hop-by-hop headers from the response" in {
      val hopByHop = Seq(
        RawHeader("Transfer-Encoding", "chunked"),
        RawHeader("Connection",        "close"),
        RawHeader("X-Application",     "keep-me")
      )
      val s0 = session().withResponse(ByteString.empty, headers = hopByHop)
      val p  = new ProxyProcessor(addVia = false)
      val result = p.processResponse(s0).futureValue

      val names = result.responseHeaders.map(_.name.toLowerCase)
      names should not contain "transfer-encoding"
      names should not contain "connection"
      names should contain ("x-application")
    }

    "strip extra response headers configured via extraStripResponse" in {
      val hdrs = Seq(
        RawHeader("X-Backend-Id", "node-42"),
        RawHeader("Content-Type", "application/json")
      )
      val s0 = session().withResponse(ByteString.empty, headers = hdrs)
      val p  = new ProxyProcessor(addVia = false, extraStripResponse = Set("X-Backend-Id"))
      val result = p.processResponse(s0).futureValue

      val names = result.responseHeaders.map(_.name.toLowerCase)
      names should not contain "x-backend-id"
    }

    // ── integration: full pipeline ─────────────────────────────────────────────

    "forward request to backend and strip hop-by-hop headers end-to-end" in {
      implicit val system: ActorSystem  = ActorSystem("proxy-processor-spec")
      implicit val mat: Materializer    = Materializer(system)
      implicit val scheduler            = system.scheduler

      try {
        var receivedVia: Option[String] = None
        var receivedProxyAuth: Boolean  = false

        val backendRoute = post {
          pathEndOrSingleSlash {
            extractRequest { req =>
              receivedVia       = req.headers.find(_.name.equalsIgnoreCase("Via")).map(_.value)
              receivedProxyAuth = req.headers.exists(_.name.equalsIgnoreCase("Proxy-Authorization"))
              complete(HttpResponse(
                entity = HttpEntity(ContentTypes.`application/json`, """{"ok":true}""")
              ))
            }
          }
        }

        val backendBinding = Http().newServerAt("127.0.0.1", 0).bind(backendRoute).futureValue
        try {
          val backendUrl = s"http://${backendBinding.localAddress.getHostString}:${backendBinding.localAddress.getPort}/"

          // ProxyProcessor sets destination from Host; no PoolProcessor needed here.
          val pipeline = ProcessorPipeline.fromSeq(
            Seq(
              new ProxyProcessor(scheme = "http", addVia = true),
              new HttpProcessor()
            ),
            "ProxyTest"
          )

          val store = new ProxyStorePipeline(pipeline, "proxy-test")

          val clientHeaders = Seq(
            RawHeader("Host",                 s"${backendBinding.localAddress.getHostString}:${backendBinding.localAddress.getPort}"),
            RawHeader("Proxy-Authorization",  "Basic dXNlcjpwYXNz"),
            RawHeader("Connection",           "keep-alive"),
            RawHeader("Keep-Alive",           "timeout=5"),
            RawHeader("X-Real-Header",        "present")
          )

          val sess = store.proxy(HttpMethods.POST, "/", ByteString("""{"test":1}"""), clientHeaders).futureValue

          sess.isRejected shouldBe false
          sess.responseBody should be (defined)
          sess.responseBody.get.utf8String should include ("true")

          // Via must have been added by the proxy
          receivedVia should be (defined)
          receivedVia.get should include ("ika-proxy")

          // Proxy-Authorization must have been stripped before forwarding
          receivedProxyAuth shouldBe false

        } finally backendBinding.unbind().futureValue
      } finally Await.result(system.terminate(), 10.seconds)
    }
  }
}
