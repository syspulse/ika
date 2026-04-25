package io.syspulse.ika.store

import scala.concurrent.Future
import scala.collection.immutable
import io.jvm.uuid._
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpMethod
import akka.util.ByteString
import io.syspulse.ika.processor.Session

trait ProxyStore {

  /** @param uriSuffix path+query after the proxy base (e.g. "/api/v1/chat/completions?x=1"). */
  def proxy(method: HttpMethod, uriSuffix: String, req: ByteString, headers: Seq[HttpHeader]): Future[Session]
}
