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

import akka.actor.Scheduler
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.settings.ClientConnectionSettings
import scala.concurrent.Await

import io.syspulse.ika.pool.ProxyPool
import io.syspulse.ika.pool.ProxySession
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.http.scaladsl.coding.Coders

object Throttler {
  val BGL = new Object()
}

class Throttler(throttle:Long, bgl:Boolean=true) {
  def block():Unit = {
    if(throttle == 0L) return

    // throttler is global
    if(bgl) {
      Throttler.BGL.synchronized {
        Thread.sleep(throttle)
      }
    } else {
      this.synchronized {
        Thread.sleep(throttle)
      }
    }
  }
}

abstract class ProxyStoreRcp(pool:ProxyPool)(implicit config:Config) extends ProxyStore {
  val log = Logger(s"${this}")

  import ProxyJson._

  // DEPRECATED: Use hardcoded defaults for legacy tests
  private val rpcThreads = 4
  private val rpcThrottle = 0L
  private val rpcTimeout = 10000L
  private val rpcDelay = 1000L
  private val rpcCompress = ""

  //implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val ec: scala.concurrent.ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(rpcThreads))

  implicit val as: ActorSystem = ActorSystem("proxy")

  implicit val sched: Scheduler = as.scheduler
  val throttler = new Throttler(rpcThrottle)

  def retry_1_deterministic(as:ActorSystem,timeout:FiniteDuration) = ConnectionPoolSettings(as)
                          .withBaseConnectionBackoff(FiniteDuration(1000,TimeUnit.MILLISECONDS))
                          .withMaxConnectionBackoff(FiniteDuration(1000,TimeUnit.MILLISECONDS))
                          .withMaxConnections(1)
                          .withMaxRetries(1)
                          .withConnectionSettings(ClientConnectionSettings(as)
                          .withIdleTimeout(timeout)
                          .withConnectingTimeout(timeout))

  def retry_deterministic(as:ActorSystem,timeout:FiniteDuration) = ConnectionPoolSettings(as)
                          .withBaseConnectionBackoff(timeout)
                          .withMaxConnectionBackoff(timeout)

  
  log.info(s"Pool: ${pool}")
  
  def parseSingleReq(req:String):Try[ProxyRpcReq] = { 
    try {
      Success(req.parseJson.convertTo[ProxyRpcReq])
    } catch {
      case e:Exception => Failure(e)        
    }      
  }

  def parseBatchReq(req:String):Try[Array[ProxyRpcReq]] = { 
    try {
      Success(req.parseJson.convertTo[Array[ProxyRpcReq]])
    } catch {
      case e:Exception => Failure(e)        
    }      
  }

  def decodeSingle(req:String) = {
    parseSingleReq(req) match {
      case Success(r) =>         
        //(r.method,r.params,r.id)
        r
      case Failure(e) => 
        log.warn(s"failed to parse: ${e}")
        ProxyRpcReq("2.0","",List(),0)
    }    
  }

  def decodeBatch(req:String) = {
    parseBatchReq(req) match {
      case Success(r) =>         
        r
      case Failure(e) => 
        log.warn(s"failed to parse: ${e}")
        Array[ProxyRpcReq]()
    } 
  }

  def getKey(r:ProxyRpcReq) = {
    s"${r.method}-${r.params.toString}"
  }

  def retry[T](f: => Future[T], delay: Long, max: Int)(implicit ec: ExecutionContext, sched: Scheduler): Future[T] = {
    f recoverWith { 
      case e if max > 0 => 
        log.error("retry: ",e)
        akka.pattern.after(FiniteDuration(delay,TimeUnit.MILLISECONDS), sched)(retry(f, delay, max - 1)) 
    }
  }

  def retry(req:String,headers:Seq[HttpHeader],session:ProxySession)(implicit ec: ExecutionContext, sched: Scheduler): Future[ProxyData] = {
    val uri = session.next()
    
    val f = rpc1(uri,req,headers,session)

    f
    .recoverWith { 
      // case e if session.retry > 0 => 
      //   // retry to the same RPC
      //   log.warn(s"retry(${session.retry},${session.lap}): ${uri}")
      //   session.failed()
      //   akka.pattern.after(FiniteDuration(rpcDelay,TimeUnit.MILLISECONDS), sched)(retry(req,session))         
      case e if session.available =>
        // switch to another RPC or fail
        log.info(s"retry(${session.retry},${session.lap}) -> ${uri}")
        session.failed()
        akka.pattern.after(FiniteDuration(rpcDelay,TimeUnit.MILLISECONDS), sched)(retry(req,headers,session))  

      // case e =>
      //   log.warn(s"??? retry(${session.retry},${session.lap}): ${uri}: ${e.getMessage()}")
      //   akka.pattern.after(FiniteDuration(rpcDelay,TimeUnit.MILLISECONDS), sched)(retry(req,session))  
    }    
  }
    
  // --------------------------------------------------------------------------------- Proxy ---
  def rpc1(uri:String,req:String,headers:Seq[HttpHeader],session:ProxySession):Future[ProxyData] = {
    log.info(s"${req.take(85)} --> ${uri}")

    // throttle here if neccessary
    throttler.block()

    val rsp = req.trim match {

      // single request
      case req if(req.startsWith("{")) => 
        single(uri,req,headers)        
      
      // batch
      case req if(req.startsWith("[")) => 
        //batchOptimized(req)
        batch(uri,req,headers,session)
        
      case _ => 
        Future {
          ProxyData(
            body = s"""{"jsonrpc": "2.0", "error": {"code": -32601, "message": "Emtpy message"}, "id": 0}""",
            src = ProxyData.LOCAL
          )
        }
    }

    rsp    
  }

  def decodeResponse(response: HttpResponse): HttpResponse = {
    log.debug(s"response: ${response.status},${response.encoding},${response.headers},${response.entity.contentLengthOption}")
    val decoder = response.encoding match {
      case HttpEncodings.gzip =>
        Coders.Gzip
      case HttpEncodings.deflate =>
        Coders.Deflate
      case HttpEncodings.identity =>
        Coders.NoCoding
      case other =>
        log.warn(s"Unknown encoding: $other")
        Coders.NoCoding
    }

    decoder.decodeMessage(response)
  }

  def http(uri:String,req:String,headers:Seq[HttpHeader]) = {                    
    val request = HttpRequest(
        HttpMethods.POST, 
        uri,
        // for some reason, Host head is empty here, QuickNode RPC rejects it with 401
        headers = headers.filter(h => h.name() != "Timeout-Access" && h.name() != "Host") ++ 
        {
          if(!rpcCompress.isBlank())
            Seq(RawHeader("Accept-Encoding", rpcCompress))
          else
            Seq()
        },
        entity = HttpEntity(ContentTypes.`application/json`,req)
      )

    log.debug(s"request: ${request} -> ${uri}")

    lazy val http = Http()
    .singleRequest(request, settings = retry_deterministic(as,FiniteDuration(rpcTimeout,TimeUnit.MILLISECONDS)))
    .map(decodeResponse)
    .flatMap(res => { 
      res.status match {
        case StatusCodes.OK => 
          val body = res.entity.dataBytes.runReduce(_ ++ _)
          body.map(d => {
            val data = d.utf8String
            log.debug(s"response: '${data}'")
            data
          })
        case _ =>
          //log.error(s"RPC error: ${res.status}")
          val body = res.entity.dataBytes.runReduce(_ ++ _)
          //val txt = Await.result(body.map(_.utf8String),FiniteDuration(5000L,TimeUnit.MILLISECONDS))
          body.map(d => {
            val data = d.utf8String
            log.warn(s"RPC response: ${res.status}: '${data}'")
            throw new Exception(s"${res.status}: ${data}")
          })
          //throw new Exception(s"${res.status}: ${txt}")
      }
    })
    .recoverWith {
      case e =>
        log.warn(s"RPC error: ${uri}: ${e.getMessage()}")
        Future.failed(e)
    }

    http
  }
 
  def single(uri:String,req:String,headers:Seq[HttpHeader]):Future[ProxyData] = {
    val r = decodeSingle(req)
    val key = getKey(r)

    // DEPRECATED: Cache removed, always fetch from remote
    for {
      rsp <- http(uri,req,headers)
      r1 <- {
        if(isError(Some(rsp))) {
          log.warn(s"uncache: ${rsp}")
          Future(ProxyData(rsp, ProxyData.REMOTE))
        } else {
          Future(ProxyData(rsp, ProxyData.REMOTE))
        }
      }
    } yield r1
  }

  def rpc(req:String,headers:Seq[HttpHeader]) = {
    log.debug(s"req='${req}', headers=${headers}")

    val session = pool.connect(req)

    retry(req, headers, session)    
  }

  def batch(uri:String,req:String,headers:Seq[HttpHeader],session:ProxySession):Future[ProxyData]

  def isError(res:Option[String]):Boolean = {
    if(! res.isDefined) 
      return true
    else {
      // fast and dirty
      // this is checked agains every request.
      (res.get.contains("""error""") && res.get.contains("""code""")) ||
      (res.get.contains(""""result":null""") || res.get.contains(""""result": null"""))
    }
  }
}
