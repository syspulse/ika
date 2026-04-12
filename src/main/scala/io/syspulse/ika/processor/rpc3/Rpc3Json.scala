package io.syspulse.ika.processor.rpc3

import io.syspulse.skel.service.JsonCommon

import spray.json.DefaultJsonProtocol

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat, deserializationError}

import io.syspulse.ika._
import io.syspulse.ika.processor.rpc3.{ProxyRpcReq, ProxyRpcRes, ProxyRpcBlockRes}
import io.syspulse.ika.processor.rpc3.{ProxyRpcBlockNumberRes, ProxyRpcBlockResultRes}

object ProxyJson extends JsonCommon {
  
  import DefaultJsonProtocol._

  implicit val jf_rpc_req = jsonFormat4(ProxyRpcReq)
  implicit val jf_rpc_res = jsonFormat3(ProxyRpcRes)
  
  implicit val jf_rpc_block_result_rsp = jsonFormat1(ProxyRpcBlockResultRes)
  implicit val jf_rpc_block_rsp = jsonFormat3(ProxyRpcBlockRes)
  implicit val jf_rpc_block_num_rsp = jsonFormat3(ProxyRpcBlockNumberRes)
}
