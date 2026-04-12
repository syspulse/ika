package io.syspulse.ika.store

import scala.util.Try
import scala.concurrent.Future
import scala.collection.immutable
import io.jvm.uuid._
import akka.http.scaladsl.model.HttpHeader

object ProxyData {
  type Source = Int

  val LOCAL = 0
  val CACHE = 1
  val REMOTE = 2
}

case class ProxyData(
  body:String,
  src:ProxyData.Source    // where data comes from
)

trait ProxyStore {

  def proxy(req:String,headers:Seq[HttpHeader]):Future[ProxyData]
}
