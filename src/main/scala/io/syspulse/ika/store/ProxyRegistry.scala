package io.syspulse.ika.store

import scala.util.{Try,Success,Failure}

import scala.collection.immutable
import com.typesafe.scalalogging.Logger
import io.jvm.uuid._
import scala.concurrent.Future

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.util.ByteString

import io.syspulse.skel.Command

import io.syspulse.ika._
import io.syspulse.ika.server._
import akka.http.scaladsl.model.{HttpHeader, HttpMethod}
import io.syspulse.ika.processor.Session

object ProxyRegistry {
  val log = Logger(s"${this}")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  final case class ProxyReq(method: HttpMethod, uriSuffix: String, req: ByteString, headers: Seq[HttpHeader], replyTo: ActorRef[Try[Session]]) extends Command  
  
  def apply(store: ProxyStore): Behavior[io.syspulse.skel.Command] = {
    registry(store)
  }

  private def registry(store: ProxyStore): Behavior[io.syspulse.skel.Command] = {    
    
    Behaviors.receiveMessage {

      case ProxyReq(method, uriSuffix, req, headers, replyTo) =>
        
        val f: Future[Session] = store.proxy(method, uriSuffix, req, headers)

        f.onComplete(r => r match {
          case Success(sess) => replyTo ! Success(sess)
          case fail @ Failure(e) => 
            log.error(s"failed to proxy: ${fail}",e)
            replyTo ! fail
        })

        Behaviors.same
    }
  }
}
