package io.syspulse.ika.processor

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader

import io.syspulse.ika.store.ProxyData

/**
 * Specs for [[Session.withRequestBody]] / [[Session.withResponse]] flowing through
 * [[RequestProcessor]] and [[ResponseProcessor]] chains (mirrors how [[io.syspulse.ika.processor.impl.HttpClientProcessor]]
 * reads `session.requestBody` after upstream rewrites).
 *
 * Request/response [[akka.http.scaladsl.model.HttpHeader]] flows the same way as for bodies
 * (`withRequestHeaders`, `addRequestHeader`, `withResponse(..., headers)`).
 */
class RequestResponseBodySpec extends AnyWordSpec with Matchers {

  implicit private val ec: ExecutionContext = ExecutionContext.global

  /** Prepends a marker so downstream sees a transformed request body. */
  private class PrefixRequestBodyProcessor(prefix: String) extends RequestProcessor {
    def name: String = "PrefixRequestBody"
    def processRequest(session: Session): Future[Session] =
      Future.successful(session.withRequestBody(prefix + session.requestBody))
  }

  /** Rewrites JSON-RPC `id` (simple string replace for tests). */
  private class RewriteJsonIdProcessor(newId: String) extends RequestProcessor {
    def name: String = "RewriteJsonId"
    def processRequest(session: Session): Future[Session] = {
      val replaced =
        session.requestBody.replaceFirst("\"id\"\\s*:\\s*\\d+", "\"id\": " + newId)
      Future.successful(session.withRequestBody(replaced))
    }
  }

  private class SetDestinationProcessor(dest: String) extends RequestProcessor {
    def name: String = "SetDestination"
    def processRequest(session: Session): Future[Session] =
      Future.successful(session.putData("destination", dest))
  }

  private class RemoveRequestHeaderProcessor(headerName: String) extends RequestProcessor {
    def name: String = "RemoveRequestHeader"
    def processRequest(session: Session): Future[Session] =
      Future.successful(session.removeRequestHeader(headerName))
  }

  private class AddRequestHeaderProcessor(header: HttpHeader) extends RequestProcessor {
    def name: String = "AddRequestHeader"
    def processRequest(session: Session): Future[Session] =
      Future.successful(session.addRequestHeader(header))
  }

  /** Replaces the value for an existing header name (case-insensitive name match). */
  private class RewriteRequestHeaderProcessor(headerName: String, newValue: String) extends RequestProcessor {
    def name: String = "RewriteRequestHeader"
    def processRequest(session: Session): Future[Session] =
      Future.successful(
        session.removeRequestHeader(headerName).addRequestHeader(RawHeader(headerName, newValue))
      )
  }

  /**
   * Simulates a backend: copies (possibly modified) request body into the response.
   * Real code uses [[io.syspulse.ika.processor.impl.HttpClientProcessor]] which sends `session.requestBody`.
   */
  private class EchoRequestAsResponseProcessor extends RequestProcessor {
    def name: String = "EchoBackend"
    def processRequest(session: Session): Future[Session] =
      Future.successful(session.withResponse("echo:" + session.requestBody, ProxyData.REMOTE))
  }

  /** Echo plus fixed response headers (e.g. as if the origin returned `X-Backend-Trace`). */
  private class EchoRequestAsResponseWithHeadersProcessor(responseHeaders: Seq[HttpHeader]) extends RequestProcessor {
    def name: String = "EchoBackendWithRespHeaders"
    def processRequest(session: Session): Future[Session] =
      Future.successful(
        session.withResponse("echo:" + session.requestBody, ProxyData.REMOTE, responseHeaders)
      )
  }

  /** Appends a suffix to an existing response body (response-phase transform). */
  private class SuffixResponseBodyProcessor(suffix: String) extends ResponseProcessor {
    def name: String = "SuffixResponseBody"
    def processResponse(session: Session): Future[Session] =
      Future.successful(
        session.responseBody match {
          case Some(body) =>
            session.copy(
              responseBody = Some(body + suffix),
              responseSource = session.responseSource,
              responseHeaderMap = session.responseHeaderMap
            )
          case None => session
        }
      )
  }

  /** Wraps JSON result in a wrapper object (response rewrite). */
  private class WrapJsonResponseProcessor extends ResponseProcessor {
    def name: String = "WrapJsonResponse"
    def processResponse(session: Session): Future[Session] =
      Future.successful(
        session.responseBody match {
          case Some(body) =>
            session.copy(
              responseBody = Some(s"""{"wrapped":true,"payload":$body}"""),
              responseSource = session.responseSource,
              responseHeaderMap = session.responseHeaderMap
            )
          case None => session
        }
      )
  }

  /** Appends a header on the response (downstream of body-producing steps). */
  private class AddResponseHeaderProcessor(header: HttpHeader) extends ResponseProcessor {
    def name: String = "AddResponseHeader"
    def processResponse(session: Session): Future[Session] =
      Future.successful(session.addResponseHeader(header))
  }

  "Request body modification" should {

    "be visible to a later RequestProcessor in the same pipeline" in {
      val pipeline = ProcessorPipeline(
        new PrefixRequestBodyProcessor("REQ:"),
        new RewriteJsonIdProcessor("99"),
        new SetDestinationProcessor("http://stub")
      )
      val in = """{"jsonrpc":"2.0","method":"x","params":[],"id":1}"""
      val result = Await.result(pipeline.process(Session(requestBody = in)), 5.seconds)

      result.requestBody shouldBe
        "REQ:" + """{"jsonrpc":"2.0","method":"x","params":[],"id": 99}"""
      result.getData[String]("destination") shouldBe Some("http://stub")
    }

    "apply before an echo backend so the echoed response reflects the rewrite" in {
      val pipeline = ProcessorPipeline(
        new RewriteJsonIdProcessor("42"),
        new SetDestinationProcessor("http://stub"),
        new EchoRequestAsResponseProcessor(),
        new SuffixResponseBodyProcessor("|tail")
      )
      val in = """{"jsonrpc":"2.0","id":1}"""
      val result = Await.result(pipeline.process(Session(requestBody = in)), 5.seconds)

      result.responseBody shouldBe Some(
        """echo:{"jsonrpc":"2.0","id": 42}|tail"""
      )
    }
  }

  "Response body modification" should {

    "run ResponseProcessor stages after the response is set" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://stub"),
        new EchoRequestAsResponseProcessor(),
        new SuffixResponseBodyProcessor("_s1"),
        new SuffixResponseBodyProcessor("_s2")
      )
      val result = Await.result(pipeline.process(Session(requestBody = "ping")), 5.seconds)

      result.responseBody shouldBe Some("echo:ping_s1_s2")
    }

    "allow wrapping JSON in a second ResponseProcessor" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://stub"),
        new EchoRequestAsResponseProcessor(),
        new WrapJsonResponseProcessor()
      )
      val result = Await.result(pipeline.process(Session(requestBody = "{}")), 5.seconds)

      result.responseBody shouldBe Some("""{"wrapped":true,"payload":echo:{}}""")
    }
  }

  "Request and response transforms" should {

    "compose: rewrite request, echo, then rewrite response" in {
      val pipeline = ProcessorPipeline(
        new PrefixRequestBodyProcessor("A:"),
        new SetDestinationProcessor("http://stub"),
        new EchoRequestAsResponseProcessor(),
        new SuffixResponseBodyProcessor(":Z")
      )
      val result = Await.result(pipeline.process(Session(requestBody = "body")), 5.seconds)

      result.requestBody should startWith("A:")
      result.responseBody shouldBe Some("echo:A:body:Z")
    }
  }

  "Request header modification" should {

    "remove headers by name (case-insensitive)" in {
      val pipeline = ProcessorPipeline(
        new RemoveRequestHeaderProcessor("X-Strip"),
        new SetDestinationProcessor("http://stub")
      )
      val headers = Seq(RawHeader("X-Keep", "ok"), RawHeader("X-Strip", "gone"), RawHeader("Other", "v"))
      val result = Await.result(
        pipeline.process(Session(requestBody = "{}", requestHeaders = headers)),
        5.seconds
      )

      result.requestHeaders.map(h => (h.name.toLowerCase, h.value)).toSet shouldBe Set(
        ("x-keep", "ok"),
        ("other", "v")
      )
    }

    "add request headers" in {
      val pipeline = ProcessorPipeline(
        new AddRequestHeaderProcessor(RawHeader("X-One", "1")),
        new AddRequestHeaderProcessor(RawHeader("X-Two", "2"))
      )
      val result = Await.result(pipeline.process(Session(requestBody = "ping")), 5.seconds)

      result.requestHeaders.map(_.name.toLowerCase).toSet should contain allOf ("x-one", "x-two")
      result.requestHeaders.find(_.is("x-one")).map(_.value) shouldBe Some("1")
      result.requestHeaders.find(_.is("x-two")).map(_.value) shouldBe Some("2")
    }

    "rewrite a header value" in {
      val pipeline = ProcessorPipeline(
        new RewriteRequestHeaderProcessor("X-Token", "new-secret")
      )
      val headers = Seq(RawHeader("X-Token", "old-secret"), RawHeader("Y", "keep"))
      val result = Await.result(
        pipeline.process(Session(requestBody = "{}", requestHeaders = headers)),
        5.seconds
      )

      result.requestHeaders.filter(_.is("x-token")).map(_.value) shouldBe Seq("new-secret")
      result.requestHeaders.exists(_.is("y")) shouldBe true
    }

    "chain add, remove, and rewrite for downstream HTTP" in {
      val pipeline = ProcessorPipeline(
        new AddRequestHeaderProcessor(RawHeader("X-A", "1")),
        new RemoveRequestHeaderProcessor("X-B"),
        new RewriteRequestHeaderProcessor("X-C", "c2"),
        new SetDestinationProcessor("http://example")
      )
      val start = Seq(
        RawHeader("X-B", "remove-me"),
        RawHeader("X-C", "c0")
      )
      val result = Await.result(
        pipeline.process(Session(requestBody = "{}", requestHeaders = start)),
        5.seconds
      )

      val names = result.requestHeaders.map(_.name.toLowerCase).toSet
      names should contain("x-a")
      names should not contain "x-b"
      result.requestHeaders.find(_.is("x-c")).map(_.value) shouldBe Some("c2")
    }
  }

  "Response header modification" should {

    "add headers after the response body is set" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://stub"),
        new EchoRequestAsResponseWithHeadersProcessor(Seq(RawHeader("X-Backend", "node-1"))),
        new AddResponseHeaderProcessor(RawHeader("X-Proxy-Added", "yes"))
      )
      val result = Await.result(pipeline.process(Session(requestBody = "hi")), 5.seconds)

      result.responseHeaders.map(h => (h.name.toLowerCase, h.value)).toSet shouldBe Set(
        ("x-backend", "node-1"),
        ("x-proxy-added", "yes")
      )
    }

    "preserve response headers through body-suffix ResponseProcessors" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://stub"),
        new EchoRequestAsResponseWithHeadersProcessor(Seq(RawHeader("X-Upstream", "u"))),
        new SuffixResponseBodyProcessor("|z"),
        new AddResponseHeaderProcessor(RawHeader("X-After-Suffix", "1"))
      )
      val result = Await.result(pipeline.process(Session(requestBody = "x")), 5.seconds)

      result.responseBody shouldBe Some("echo:x|z")
      result.responseHeaders.map(_.name.toLowerCase).toSet should contain allOf ("x-upstream", "x-after-suffix")
    }
  }
}
