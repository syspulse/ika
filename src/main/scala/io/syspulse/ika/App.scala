package io.syspulse.ika

import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.concurrent.Await

import io.syspulse.skel
import io.syspulse.skel.util.Util
import io.syspulse.skel.config._

import io.syspulse.ika._
import io.syspulse.ika.store._
import io.syspulse.ika.pool._
import io.syspulse.ika.processor._
import io.syspulse.ika.server.ProxyRoutes
import akka.actor.ActorSystem

import io.jvm.uuid._

import io.syspulse.skel.FutureAwaitable._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

case class Config(
  // HTTP Server
  host:String="0.0.0.0",
  port:Int=8080,
  uri:String = "/api/v1/ika",

  // Core components (generic)
  datastore:String = "pipeline://",  // pipeline://, rpc://, simple://, none://
  telemetry:String = "stdout://60",  // stdout://, prometheus://, log://
  
  // Security
  apiKey:String = "",                // API key suffix to URL

  // Command and destinations
  cmd:String = "proxy",
  destinations: Seq[String] = Seq("http://localhost:8545"),  // Backend destinations
)

object App extends skel.Server {
  
  def main(args:Array[String]):Unit = {
    Console.err.println(s"args: '${args.mkString(",")}'")

    val d = Config()
    val c = Configuration.withPriority(Seq(
      new ConfigurationAkka,
      new ConfigurationProp,
      new ConfigurationEnv, 
      new ConfigurationArgs(args,"squid3","",
        ArgString('h', "http.host",s"listen host (def: ${d.host})"),
        ArgInt('p', "http.port",s"listern port (def: ${d.port})"),
        ArgString('u', "http.uri",s"api uri (def: ${d.uri})"),

        ArgString('d', "datastore",s"Datastore [none://,rpc://,pipeline://,pipeline://web3,pipeline://simple] (def: ${d.datastore})"),        

        ArgString('_', "api.key",s"Cache [none,time://] (def: ${d.apiKey})"),
        
        ArgCmd("proxy","Command"),
        ArgCmd("server","Command"),

        // ArgCmd("client","Command"),
        ArgParam("<rpc,...>","List of RPC nodes (added to --pool)"),
        ArgLogging()
      ).withExit(1)
    )).withLogging()

    implicit val config = Config(
      host = c.getString("http.host").getOrElse(d.host),
      port = c.getInt("http.port").getOrElse(d.port),
      uri = c.getString("http.uri").getOrElse(d.uri),
      
      datastore = c.getString("datastore").getOrElse(d.datastore),      

      apiKey = c.getString("api.key").getOrElse(d.apiKey),
      
      cmd = c.getCmd().getOrElse(d.cmd),
      destinations = c.getParams(),
    )

    Console.err.println(s"Config: ${config}")    
        
    // Create ActorSystem for pipeline (needed for HttpClientProcessor)
    implicit val actorSystem: ActorSystem = ActorSystem("ika-pipeline")
    implicit val ec: scala.concurrent.ExecutionContext = actorSystem.dispatcher

    val store = try { config.datastore.split("://").toList match {

      // Pipeline mode - supports profiles
      case "pipeline" :: Nil => ProxyStorePipeline("web3")
      case "pipeline" :: profile :: _ => ProxyStorePipeline(profile)

      case "none" :: _ => new ProxyStoreNone()
      case _ =>
        Console.err.println(s"Uknown datastore: '${config.datastore}'")
        sys.exit(1)
    }} catch {
      case e:Exception =>
        log.error(s"Failed to create store",e)
        sys.exit(1)
    }
    
    config.cmd match {
      case "proxy" | "server" => 

        run( config.host, config.port,config.uri,c,
          Seq(
            (ProxyRegistry(store),"ProxyRegistry",(r, ac) => new ProxyRoutes(r)(ac,config) )
          )
        ) 
    }
  }
}
