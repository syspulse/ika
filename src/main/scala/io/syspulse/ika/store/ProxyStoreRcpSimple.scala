package io.syspulse.ika.store

import scala.util.Try
import scala.util.{Success,Failure}
import scala.collection.immutable

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.scalalogging.Logger

import akka.http.scaladsl.Http

import io.jvm.uuid._
import scala.concurrent.Future

import spray.json._
import io.syspulse.ika.processor.rpc3.ProxyRpcReq
import io.syspulse.ika.processor.rpc3.ProxyJson

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.StatusCodes

import io.syspulse.ika.Config
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.ContentTypes

import io.syspulse.ika.pool.ProxyPool
import io.syspulse.ika.pool.ProxySession
import akka.http.scaladsl.model.HttpHeader

class ProxyStoreRcpSimple(pool:ProxyPool)(implicit config:Config)
  extends ProxyStoreRcp(pool)(config) {

  def batch(uri:String,req:String,headers:Seq[HttpHeader],session:ProxySession):Future[ProxyData] = {
    // DEPRECATED: Cache removed, always fetch from remote
    http(uri,req,headers).map(r => ProxyData(r, ProxyData.REMOTE))
  }

}

