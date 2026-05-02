package io.syspulse.ika.server

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import io.syspulse.ika.processor.ProcessorPipeline
import io.syspulse.ika.processor.ai.AITokensProcessor
import io.syspulse.ika.processor.impl.{HeaderProcessor, HttpProcessor, PoolProcessor, RetryProcessor}
import io.syspulse.ika.store.ProxyStorePipeline
import io.syspulse.ika.telemetry.Telemetry

/**
 * Integration tests for SSE (Server-Sent Events) proxy support.
 *
 * Tests:
 *   1. SSE pass-through: backend sends text/event-stream, client receives all SSE chunks
 *   2. AITokensProcessor SSE: usage tokens extracted from final SSE event, recorded in telemetry
 */
class ProxySseIntegrationSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 100.millis)

  private def withSystem[A](body: ActorSystem => A): A = {
    val system = ActorSystem(s"sse-test-${System.nanoTime()}")
    try body(system)
    finally Await.result(system.terminate(), 10.seconds)
  }

  // SSE events the mock backend will emit
  private val sseChunks = Seq(
    """data: {"id":"chatcmpl-1","choices":[{"delta":{"content":"Hello"}}]}""" + "\n\n",
    """data: {"id":"chatcmpl-1","choices":[{"delta":{"content":" world"}}]}""" + "\n\n",
    // Final chunk carries usage
    """data: {"id":"chatcmpl-1","choices":[{"delta":{}}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}""" + "\n\n",
    "data: [DONE]\n\n"
  )

  private val sseContentType: ContentType = MediaType.text("event-stream").withMissingCharset

  "SSE proxy" should {

    "pass through all SSE chunks to the client" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.dispatcher
      implicit val mat: Materializer    = Materializer(system)
      implicit val scheduler            = system.scheduler

      val backendRoute = post {
        pathEndOrSingleSlash {
          entity(as[String]) { _ =>
            val source: Source[ByteString, _] =
              Source(sseChunks.map(ByteString(_)))
            complete(HttpResponse(
              status  = StatusCodes.OK,
              entity  = HttpEntity(sseContentType, source)
            ))
          }
        }
      }

      val backendBinding = Http().newServerAt("127.0.0.1", 0).bind(backendRoute).futureValue
      try {
        val backendUrl = s"http://${backendBinding.localAddress.getHostString}:${backendBinding.localAddress.getPort}/"

        val pipeline = ProcessorPipeline.fromSeq(
          Seq(
            PoolProcessor.roundRobin(Seq(backendUrl)),
            new RetryProcessor(maxRetries = 0, delayMs = 0L),
            new HttpProcessor()
          ),
          "SsePassThrough"
        )

        val store        = new ProxyStorePipeline(pipeline, "sse-passthrough")
        val proxyBinding = Http().newServerAt("127.0.0.1", 0)
          .bind(post {
            pathEndOrSingleSlash {
              entity(as[String]) { body =>
                onSuccess(store.proxy(HttpMethods.POST, "/", ByteString(body), Nil)) { sess =>
                  if (sess.isStreaming) {
                    val stream = sess.responseStream.getOrElse(Source.empty[ByteString])
                    complete(HttpResponse(
                      status = sess.responseStatus,
                      entity = HttpEntity.Chunked.fromData(sess.responseContentType, stream)
                    ))
                  } else {
                    complete(HttpResponse(
                      status = sess.responseStatus,
                      entity = HttpEntity(sess.responseContentType,
                        sess.responseBody.getOrElse(ByteString.empty))
                    ))
                  }
                }
              }
            }
          })
          .futureValue

        try {
          val proxyUrl = s"http://${proxyBinding.localAddress.getHostString}:${proxyBinding.localAddress.getPort}/"

          val resp = Http()
            .singleRequest(HttpRequest(
              HttpMethods.POST,
              Uri(proxyUrl),
              entity = HttpEntity(ContentTypes.`application/json`, """{"model":"gpt-4","stream":true}""")
            ))
            .futureValue

          resp.status shouldBe StatusCodes.OK
          resp.entity.contentType.mediaType.subType.toLowerCase shouldBe "event-stream"

          val body = resp.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).futureValue.utf8String
          body should include("Hello")
          body should include("world")
          body should include("[DONE]")
          body should include("prompt_tokens")

        } finally proxyBinding.unbind().futureValue
      } finally backendBinding.unbind().futureValue
    }

    "extract token usage from SSE stream into telemetry via AITokensProcessor" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.dispatcher
      implicit val mat: Materializer    = Materializer(system)
      implicit val scheduler            = system.scheduler

      val backendRoute = post {
        pathEndOrSingleSlash {
          entity(as[String]) { _ =>
            val source: Source[ByteString, _] =
              Source(sseChunks.map(ByteString(_)))
            complete(HttpResponse(
              status = StatusCodes.OK,
              entity = HttpEntity(sseContentType, source)
            ))
          }
        }
      }

      val backendBinding = Http().newServerAt("127.0.0.1", 0).bind(backendRoute).futureValue
      try {
        val backendUrl = s"http://${backendBinding.localAddress.getHostString}:${backendBinding.localAddress.getPort}/"

        val telemetry = new Telemetry()
        val aiTokens  = new AITokensProcessor()(ec)

        val pipeline = ProcessorPipeline.fromSeq(
          Seq(
            aiTokens,
            PoolProcessor.roundRobin(Seq(backendUrl)),
            new RetryProcessor(maxRetries = 0, delayMs = 0L),
            new HttpProcessor()
          ),
          "SseTokens"
        )

        val store = new ProxyStorePipeline(pipeline, "sse-tokens")

        // Attach telemetry to the session via processorData.
        // We test by directly calling store.proxy and consuming the stream.
        val initSession = io.syspulse.ika.processor.Session(
          requestBody    = ByteString("""{"model":"gpt-4","stream":true}"""),
          requestHeaders = Nil
        ).putData("telemetry", telemetry)
          .putData("provider", "openai")
          .putData("model", "gpt-4")
          .putData("http.method", "POST")
          .putData("http.uriSuffix", "/")

        val sess = pipeline.process(initSession).futureValue

        sess.isStreaming shouldBe true

        // Consume the stream to trigger the telemetry sink
        val streamData = sess.responseStream.get
          .runFold(ByteString.empty)(_ ++ _)
          .futureValue
          .utf8String

        streamData should include("[DONE]")

        // Allow the async telemetry callback to fire
        Thread.sleep(200)

        val tokens = telemetry.getAiTokens
        val (inputTok, outputTok) = tokens.getOrElse("openai", Map.empty).getOrElse("gpt-4", (0L, 0L))
        inputTok  shouldBe 10L
        outputTok shouldBe 5L

      } finally backendBinding.unbind().futureValue
    }
  }
}
