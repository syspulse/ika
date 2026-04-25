package io.syspulse.ika.server

import com.typesafe.scalalogging.Logger
import io.jvm.uuid._
import scala.util.{Try,Success,Failure}
import java.nio.file.Paths

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.FileIO

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.headers.`Content-Type`
import akka.http.scaladsl.server.RejectionHandler
import akka.http.scaladsl.model.StatusCodes._

import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings

import io.swagger.v3.oas.annotations.enums.ParameterIn
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.{Operation, Parameter}
import io.swagger.v3.oas.annotations.parameters.RequestBody
// import javax.ws.rs.{Consumes, POST, GET, DELETE, Path, Produces}
// import javax.ws.rs.core.MediaType
import jakarta.ws.rs.{Consumes, POST, PUT, GET, DELETE, Path, Produces}
import jakarta.ws.rs.core.MediaType


import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter

import io.syspulse.skel.service.Routeable
import io.syspulse.skel.service.CommonRoutes

import io.syspulse.skel.Command

// import io.syspulse.skel.auth.permissions.Permissions
// import io.syspulse.skel.auth.RouteAuthorizers

import io.syspulse.ika._
import io.syspulse.ika.store.ProxyRegistry
import io.syspulse.ika.store.ProxyRegistry._
import io.syspulse.ika.server._
import io.syspulse.skel.service.telemetry.TelemetryRegistry
import akka.http.scaladsl.server.AuthorizationFailedRejection

@Path("/")
class ProxyRoutes(registry: ActorRef[Command])(implicit context: ActorContext[_],config:Config) extends CommonRoutes with Routeable {
  //with RouteAuthorizers {
  
  implicit val system: ActorSystem[_] = context.system
  
  // implicit val permissions = Permissions()

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import io.syspulse.ika.processor.rpc3.ProxyJson._
  
  val metricGetCount: Counter = Counter.build().name("ika_get_total").help("ika gets").register(TelemetryRegistry.registry)
  val metricPostCount: Counter = Counter.build().name("ika_post_total").help("ika posts").register(TelemetryRegistry.registry)
  val metricPutCount: Counter = Counter.build().name("ika_put_total").help("ika puts").register(TelemetryRegistry.registry)
  val metricDeleteCount: Counter = Counter.build().name("ika_delete_total").help("ika deletes").register(TelemetryRegistry.registry)
  val metricOptionsCount: Counter = Counter.build().name("ika_options_total").help("ika options").register(TelemetryRegistry.registry)
  
  private def proxy(method: HttpMethod, uriSuffix: String, req: akka.util.ByteString, headers: Seq[HttpHeader]): Future[Try[io.syspulse.ika.processor.Session]] =
    registry.ask(ProxyReq(method, uriSuffix, req, headers, _))

  private def normalizeSuffixPath(p: String): String = {
    val s = Option(p).getOrElse("").trim
    if (s.isEmpty) "/"
    else if (s.startsWith("/")) s
    else s"/$s"
  }

  private def rpcRoute(uriSuffixPath: String) = extractRequest { request =>
    val strictF = request.entity.toStrict(15.seconds).map(_.data)
    onSuccess(strictF) { reqBody =>
      val method = request.method
      val headers = request.headers
      val suffixQuery = request.uri.rawQueryString.map(q => s"?$q").getOrElse("")
      val uriSuffix = s"${normalizeSuffixPath(uriSuffixPath)}${suffixQuery}"

      onSuccess(proxy(method, uriSuffix, reqBody, headers)) { rsp =>
        method match {
          case HttpMethods.GET     => metricGetCount.inc()
          case HttpMethods.POST    => metricPostCount.inc()
          case HttpMethods.PUT     => metricPutCount.inc()
          case HttpMethods.DELETE  => metricDeleteCount.inc()
          case HttpMethods.OPTIONS => metricOptionsCount.inc()
          case _                   => // ignore
        }
        rsp match {
          case Success(sess) =>
            val body = sess.responseBody.getOrElse(akka.util.ByteString.empty)
            // Drop hop-by-hop headers that must not be forwarded by proxies
            val hopByHop = Set(
              "connection",
              "keep-alive",
              "proxy-authenticate",
              "proxy-authorization",
              "te",
              "trailer",
              "transfer-encoding",
              "upgrade"
            )
            val filtered = sess.responseHeaders.filterNot(h => hopByHop.contains(h.lowercaseName()))
            complete(
              HttpResponse(
                status = sess.responseStatus,
                headers = filtered.toList,
                entity = HttpEntity(sess.responseContentType, body)
              )
            )
          case Failure(e) =>
            complete(
              HttpResponse(
                status = StatusCodes.InternalServerError,
                entity = HttpEntity(ContentTypes.`application/json`, s"""{"error":"proxy_failed","message":"${e.getMessage}"}""")
              )
            )
        }
      }
    }
  }
  
  val corsAllow = CorsSettings(system.classicSystem)
    //.withAllowGenericHttpRequests(true)
    .withAllowCredentials(true)
    .withAllowedMethods(Seq(HttpMethods.OPTIONS,HttpMethods.GET,HttpMethods.POST,HttpMethods.PUT,HttpMethods.DELETE,HttpMethods.HEAD))

  override def routes: Route = cors(corsAllow) {
    if (config.apiKey.isBlank()) {
      // No API key: everything after the proxy base URI is suffix.
      concat(
        pathEndOrSingleSlash {
          rpcRoute("/")
        },
        pathPrefix(Remaining) { rest =>
          rpcRoute(rest)
        }
      )
    } else {
      // API key is a mandatory segment right after the proxy base URI.
      pathPrefix(Segment) { apiKey =>
        if (config.apiKey == apiKey) {
          concat(
            pathEndOrSingleSlash {
              rpcRoute("/")
            },
            pathPrefix(Remaining) { rest =>
              rpcRoute(rest)
            }
          )
        } else reject(AuthorizationFailedRejection)
      }
    }
  }
}
