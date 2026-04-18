package io.syspulse.ika.store

import com.typesafe.scalalogging.Logger

import scala.concurrent.Future

import spray.json._
import akka.http.scaladsl.model.{HttpHeader, HttpMethod}

import io.syspulse.ika.processor.{ResponseSource, Session}
import io.syspulse.ika.processor.rpc3.ProxyRpcReq
import io.syspulse.ika.processor.rpc3.ProxyJson

class ProxyStoreNone extends ProxyStore {
  val log = Logger(s"${this}")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  import ProxyJson._
  
  def proxy(method: HttpMethod, uriSuffix: String, req: String, headers: Seq[HttpHeader]): Future[Session] = {
    log.info(s"req='${req}', headers=${headers}")

    val request = if(req.trim.startsWith("{")) {
      req.parseJson.convertTo[ProxyRpcReq]
    } else
      ProxyRpcReq(jsonrpc = "",method = "",params = List.empty,id = 100)

    Future.successful(
      Session(requestBody = req, requestHeaders = headers)
        .withResponse(
          s"""{"jsonrpc": "2.0", "error": {"code": -32601, "message": "Not implemented"}, "id": ${request.id}}""",
          ResponseSource.LOCAL
        )
    )
  }
}
