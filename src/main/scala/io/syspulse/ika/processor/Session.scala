package io.syspulse.ika.processor

import java.util.Locale

import akka.http.scaladsl.model.HttpHeader
import io.syspulse.ika.store.ProxyData

/**
 * Rejection represents a pipeline failure.
 *
 * The rejection is abstract and does not assume any specific format (JSON-RPC, REST, etc.).
 * A RejectionProcessor in the pipeline is responsible for converting the rejection
 * into an appropriate response format and setting the HTTP response status code.
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
 *
 * Request and response headers are stored as [[scala.collection.immutable.Map]] keyed by
 * lower-cased field name ([[java.util.Locale.ROOT]]); each name appears at most once.
 * [[requestHeaders]] / [[responseHeaders]] expose the current [[akka.http.scaladsl.model.HttpHeader]]s
 * as a sequence (order is not specified).
 */
case class Session private[processor] (
  // Request data
  requestBody: String,
  requestHeaderMap: Map[String, HttpHeader],

  // Response data
  responseBody: Option[String],
  responseHeaderMap: Map[String, HttpHeader],
  responseSource: ProxyData.Source,

  // Pipeline control
  rejection: Option[Rejection],

  // Inter-processor communication
  // Processors can store/read arbitrary data here for downstream/upstream processors
  // Examples: "destination", "pool", "retry", "maxRetry", "timeoutMs", "cacheHit", etc.
  processorData: Map[String, Any],

  // Timing data (for telemetry)
  startTime: Long,
  endTime: Option[Long]
) {

  /** All request headers (order not specified). */
  def requestHeaders: Seq[HttpHeader] = requestHeaderMap.values.toSeq

  /** All response headers (order not specified). */
  def responseHeaders: Seq[HttpHeader] = responseHeaderMap.values.toSeq

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
   * Replace request headers (built from a sequence: duplicate field names collapse, last wins).
   */
  def withRequestHeaders(headers: Seq[HttpHeader]): Session = {
    copy(requestHeaderMap = Session.headersFromSeq(headers))
  }

  /**
   * Add or replace a request header (same field name as an existing header, any casing, is replaced).
   */
  def addRequestHeader(header: HttpHeader): Session = {
    val k = Session.headerKey(header)
    copy(requestHeaderMap = requestHeaderMap + (k -> header))
  }

  /**
   * Remove a request header by field name (case-insensitive).
   */
  def removeRequestHeader(name: String): Session = {
    copy(requestHeaderMap = requestHeaderMap - name.toLowerCase(Locale.ROOT))
  }

  /**
   * Add or replace a response header (same field name as an existing header, any casing, is replaced).
   */
  def addResponseHeader(header: HttpHeader): Session = {
    val k = Session.headerKey(header)
    copy(responseHeaderMap = responseHeaderMap + (k -> header))
  }

  /**
   * Set response data
   */
  def withResponse(body: String, source: ProxyData.Source = ProxyData.REMOTE, headers: Seq[HttpHeader] = Seq.empty): Session = {
    copy(
      responseBody = Some(body),
      responseSource = source,
      responseHeaderMap = Session.headersFromSeq(headers)
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

object Session {

  def headerKey(h: HttpHeader): String =
    h.name.toLowerCase(Locale.ROOT)

  /**
   * Build a map keyed by lower-cased field name; if the same name appears multiple times in `headers`,
   * the last occurrence wins (matching HTTP single-value field semantics for our store).
   */
  def headersFromSeq(headers: Seq[HttpHeader]): Map[String, HttpHeader] =
    headers.map(h => headerKey(h) -> h).toMap

  def apply(
    requestBody: String,
    requestHeaders: Seq[HttpHeader] = Seq.empty,
    responseBody: Option[String] = None,
    responseHeaders: Seq[HttpHeader] = Seq.empty,
    responseSource: ProxyData.Source = ProxyData.LOCAL,
    rejection: Option[Rejection] = None,
    processorData: Map[String, Any] = Map.empty,
    startTime: Long = System.currentTimeMillis(),
    endTime: Option[Long] = None
  ): Session =
    new Session(
      requestBody,
      headersFromSeq(requestHeaders),
      responseBody,
      headersFromSeq(responseHeaders),
      responseSource,
      rejection,
      processorData,
      startTime,
      endTime
    )
}
