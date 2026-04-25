package io.syspulse.ika.store

import com.typesafe.scalalogging.Logger

import scala.concurrent.Future

import spray.json._
import akka.http.scaladsl.model.{HttpHeader, HttpMethod}
import akka.util.ByteString

import io.syspulse.ika.processor.{ResponseSource, Session}
import io.syspulse.ika.processor.rpc3.ProxyRpcReq
import io.syspulse.ika.processor.rpc3.ProxyJson

class ProxyStoreNone extends ProxyStore {
  val log = Logger(s"${this}")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  import ProxyJson._
  
  def proxy(method: HttpMethod, uriSuffix: String, req: ByteString, headers: Seq[HttpHeader]): Future[Session] = {
    log.info(s"req='${req.size} bytes', headers=${headers}")

    val reqString = req.utf8String
    val request = if(reqString.trim.startsWith("{")) {
      reqString.parseJson.convertTo[ProxyRpcReq]
    } else
      ProxyRpcReq(jsonrpc = "",method = "",params = List.empty,id = 100)

    Future.successful(
      Session(requestBody = req, requestHeaders = headers)
        .withResponse(
          ByteString(s"""{"jsonrpc": "2.0", "error": {"code": -32601, "message": "Not implemented"}, "id": ${request.id}}"""),
          ResponseSource.LOCAL
        )
    )
  }
}
