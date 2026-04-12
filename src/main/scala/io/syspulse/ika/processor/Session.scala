package io.syspulse.ika.processor

import akka.http.scaladsl.model.HttpHeader
import io.syspulse.ika.store.ProxyData

/**
 * Rejection represents a pipeline failure.
 *
 * The rejection is abstract and does not assume any specific format (JSON-RPC, REST, etc.).
 * A RejectionProcessor in the pipeline is responsible for converting the rejection
 * into an appropriate response format and setting the HTTP status code.
 *
 * @param code Error code (can be JSON-RPC code, HTTP status code, or custom code)
 * @param message Human-readable error message
 * @param processorName Name of the processor that rejected the request
 * @param details Optional additional details about the rejection
 */
case class Rejection(
  code: Int,
  message: String,
  processorName: String,
  details: Option[String] = None
)

/**
 * Session is the immutable state container that flows through the processor pipeline.
 * Each processor can read from the session and return a modified copy.
 *
 * The session carries:
 * - Request data (body, headers)
 * - Response data (body, headers, source)
 * - Pipeline control (rejection)
 * - Inter-processor communication (processorData)
 * - Timing data (for telemetry)
 *
 * All processor-specific data (destination, pool, retry counters, timeouts, etc.)
 * should be stored in processorData.
 */
case class Session(
  // Request data
  requestBody: String,
  requestHeaders: Seq[HttpHeader] = Seq.empty,

  // Response data
  responseBody: Option[String] = None,
  responseHeaders: Seq[HttpHeader] = Seq.empty,
  responseSource: ProxyData.Source = ProxyData.LOCAL,

  // Pipeline control
  rejection: Option[Rejection] = None,

  // Inter-processor communication
  // Processors can store/read arbitrary data here for downstream/upstream processors
  // Examples: "destination", "pool", "retry", "maxRetry", "timeoutMs", "cacheHit", etc.
  processorData: Map[String, Any] = Map.empty,

  // Timing data (for telemetry)
  startTime: Long = System.currentTimeMillis(),
  endTime: Option[Long] = None
) {

  /**
   * Mark this session as rejected with given error details
   */
  def reject(code: Int, message: String, processorName: String, details: Option[String] = None): Session = {
    copy(rejection = Some(Rejection(code, message, processorName, details)))
  }

  /**
   * Check if session is rejected
   */
  def isRejected: Boolean = rejection.isDefined

  /**
   * Store processor-specific data for downstream processors
   */
  def putData(key: String, value: Any): Session = {
    copy(processorData = processorData + (key -> value))
  }

  /**
   * Retrieve processor-specific data
   */
  def getData[T](key: String): Option[T] = {
    processorData.get(key).map(_.asInstanceOf[T])
  }

  /**
   * Update request body (for upstream processors to modify before HTTP)
   */
  def withRequestBody(body: String): Session = {
    copy(requestBody = body)
  }

  /**
   * Update request headers (for upstream processors to modify before HTTP)
   */
  def withRequestHeaders(headers: Seq[HttpHeader]): Session = {
    copy(requestHeaders = headers)
  }

  /**
   * Add a request header (for upstream processors to add headers)
   */
  def addRequestHeader(header: HttpHeader): Session = {
    copy(requestHeaders = requestHeaders :+ header)
  }

  /**
   * Set response data
   */
  def withResponse(body: String, source: ProxyData.Source = ProxyData.REMOTE, headers: Seq[HttpHeader] = Seq.empty): Session = {
    copy(
      responseBody = Some(body),
      responseSource = source,
      responseHeaders = headers
    )
  }

  /**
   * Set response body only
   */
  def withResponseBody(body: String): Session = {
    copy(responseBody = Some(body))
  }

  /**
   * Set response source
   */
  def withResponseSource(source: ProxyData.Source): Session = {
    copy(responseSource = source)
  }

  /**
   * Mark session as complete
   */
  def complete(): Session = {
    copy(endTime = Some(System.currentTimeMillis()))
  }

  /**
   * Get duration in milliseconds
   */
  def durationMs: Long = {
    endTime.getOrElse(System.currentTimeMillis()) - startTime
  }

  /**
   * Create a ProxyData from this session's response
   */
  def toProxyData: ProxyData = {
    ProxyData(
      body = responseBody.getOrElse(""),
      src = responseSource
    )
  }
}
