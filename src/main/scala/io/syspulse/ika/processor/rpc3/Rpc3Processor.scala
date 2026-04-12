package io.syspulse.ika.processor.rpc3

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{RequestProcessor, Session}

/**
 * Rpc3Processor handles RPC3/JSON-RPC specific request preprocessing.
 *
 * QuickNode-specific fixes:
 * - Removes "Host" header (QuickNode rejects empty Host headers with 401)
 * - Removes "Timeout-Access" header (internal header not for external RPC)
 *
 * This processor should be placed before HttpClientProcessor in the pipeline
 * to ensure RPC3 requests are properly formatted for QuickNode and other
 * JSON-RPC providers that are sensitive to header formatting.
 *
 * Pipeline position:
 * [...] → Rpc3Processor → HttpClientProcessor → [...]
 */
class Rpc3Processor(implicit ec: ExecutionContext) extends RequestProcessor {

  override val name: String = "Rpc3"

  private val log = Logger(name)

  // Headers to filter out before sending to RPC3 providers
  private val filteredHeaders = Set(
    "host",           // QuickNode rejects empty Host headers with 401
    "timeout-access"  // Internal header, not for external RPC
  )

  override def processRequest(session: Session): Future[Session] = {
    // Filter out problematic headers for QuickNode and other RPC3 providers
    val originalHeaderCount = session.requestHeaders.size

    val cleanedSession = session.requestHeaders.foldLeft(session) { (sess, header) =>
      val headerNameLower = header.name.toLowerCase
      if (filteredHeaders.contains(headerNameLower)) {
        log.debug(s"Filtering header: ${header.name}")
        sess.removeRequestHeader(header.name)
      } else {
        sess
      }
    }

    val filteredCount = originalHeaderCount - cleanedSession.requestHeaders.size
    if (filteredCount > 0) {
      log.debug(s"Filtered $filteredCount headers for RPC3 compatibility")
    }

    Future.successful(cleanedSession)
  }
}

object Rpc3Processor {
  def apply()(implicit ec: ExecutionContext): Rpc3Processor = {
    new Rpc3Processor()
  }
}
