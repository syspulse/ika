package io.syspulse.ika.store

import scala.util.{Try,Success,Failure}

import scala.collection.immutable
import com.typesafe.scalalogging.Logger
import io.jvm.uuid._
import scala.concurrent.Future

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import io.syspulse.skel.Command

import io.syspulse.ika._
import io.syspulse.ika.server._
import akka.http.scaladsl.model.HttpHeader


object ProxyRegistry {
  val log = Logger(s"${this}")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    
  final case class ProxyRpc(req:String,headers:Seq[HttpHeader],replyTo: ActorRef[Try[String]]) extends Command  
  
  def apply(store: ProxyStore): Behavior[io.syspulse.skel.Command] = {
    registry(store)
  }

  private def registry(store: ProxyStore): Behavior[io.syspulse.skel.Command] = {    
    
    Behaviors.receiveMessage {

      case ProxyRpc(req,headers,replyTo) =>
        
        val f: Future[ProxyData] = store.rpc(req,headers)
        val b: Future[String] = f.map(r => r.body)

        b.onComplete(r => r match {
          case Success(rsp) => replyTo ! Success(rsp)
          case fail @ Failure(e) => 
            log.error(s"failed to proxy: ${fail}",e)
            replyTo ! fail
        })

        Behaviors.same
    }
  }
}
