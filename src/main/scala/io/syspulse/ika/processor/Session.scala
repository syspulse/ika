package io.syspulse.ika.processor

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.{ContentType, ContentTypes, StatusCode, StatusCodes}
import akka.util.ByteString

/**
 * Session state controls pipeline flow.
 *
 * - CONTINUE: Continue processing to the next processor (default)
 * - RETURN: Stop processing successfully, return current session (e.g., cache hit)
 * - REJECT: Stop processing with error, session has rejection details
 */
object SessionState extends Enumeration {
  type SessionState = Value
  val CONTINUE, RETURN, REJECT = Value
}

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
 *
 * Pipeline control via state:
 * - CONTINUE: Process next processor
 * - RETURN: Stop pipeline successfully (e.g., cache hit)
 * - REJECT: Stop pipeline with error
 */
case class Session private[processor] (
  // Request data
  requestBody: ByteString,
  requestHeaderMap: Map[String, HttpHeader],

  // Response data
  responseBody: Option[ByteString],
  responseHeaderMap: Map[String, HttpHeader],
  responseSource: ResponseSource.Source,
  responseStatus: StatusCode,
  responseContentType: ContentType,

  // Pipeline control
  state: SessionState.SessionState,
  rejection: Option[Rejection],

  // Inter-processor communication
  // Processors can store/read arbitrary data here for downstream/upstream processors
  // Examples: "destination", "pool", "retry", "maxRetry", "timeoutMs", "cacheHit", etc.
  processorData: Map[String, Any],

  // Timing data (for telemetry)
  startTime: Long,
  endTime: Option[Long],

  // Pipeline execution cursor (mutable, thread-safe)
  cursor: AtomicInteger,
  // Pipeline processors list for next() dispatch
  pipeline: Seq[Processor]
) {

  /** All request headers (order not specified). */
  def requestHeaders: Seq[HttpHeader] = requestHeaderMap.values.toSeq

  /** All response headers (order not specified). */
  def responseHeaders: Seq[HttpHeader] = responseHeaderMap.values.toSeq

  /**
   * Mark this session as rejected with given error details
   */
  def reject(code: Int, message: String, processorName: String, details: Option[String] = None): Session = {
    copy(
      state = SessionState.REJECT,
      rejection = Some(Rejection(code, message, processorName, details))
    )
  }

  /**
   * Mark this session to return early (stop processing, success)
   * Used by CacheProcessor when cache hit occurs
   */
  def returnEarly(reason: String = "early_return"): Session = {
    copy(state = SessionState.RETURN)
      .putData("returnReason", reason)
  }

  /**
   * Check if session is rejected
   */
  def isRejected: Boolean = state == SessionState.REJECT

  /**
   * Check if session should return early (stop processing)
   */
  def shouldReturn: Boolean = state == SessionState.RETURN

  /**
   * Check if session should continue processing
   */
  def shouldContinue: Boolean = state == SessionState.CONTINUE

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
   * Remove processor-specific data.
   */
  def removeData(key: String): Session = {
    copy(processorData = processorData - key)
  }

  /**
   * Update request body (for upstream processors to modify before HTTP)
   */
  def withRequestBody(body: ByteString): Session = {
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
   * Remove a response header by field name (case-insensitive).
   */
  def removeResponseHeader(name: String): Session = {
    copy(responseHeaderMap = responseHeaderMap - name.toLowerCase(Locale.ROOT))
  }

  /**
   * Set response data
   */
  def withResponse(
    body: ByteString,
    source: ResponseSource.Source = ResponseSource.REMOTE,
    status: StatusCode = StatusCodes.OK,
    headers: Seq[HttpHeader] = Seq.empty,
    contentType: ContentType = ContentTypes.`application/json`
  ): Session = {
    copy(
      responseBody = Some(body),
      responseSource = source,
      responseStatus = status,
      responseHeaderMap = Session.headersFromSeq(headers),
      responseContentType = contentType
    )
  }

  /**
   * Set response body only
   */
  def withResponseBody(body: ByteString): Session = {
    copy(responseBody = Some(body))
  }

  /**
   * Set response source
   */
  def withResponseSource(source: ResponseSource.Source): Session = {
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

  /** Set pipeline processors and reset cursor to before the first processor. */
  def withPipeline(processors: Seq[Processor]): Session =
    copy(pipeline = processors, cursor = new AtomicInteger(-1))

  /** Set cursor to a specific processor index (used by retry logic). */
  def withCursor(i: Int): Session = {
    cursor.set(i)
    this
  }

  /** Get the next processor based on cursor; advances cursor atomically. */
  def nextProcessor: Option[Processor] = {
    val nextIdx = cursor.incrementAndGet()
    pipeline.lift(nextIdx)
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
    requestBody: ByteString,
    requestHeaders: Seq[HttpHeader] = Seq.empty,
    responseBody: Option[ByteString] = None,
    responseHeaders: Seq[HttpHeader] = Seq.empty,
    responseSource: ResponseSource.Source = ResponseSource.LOCAL,
    responseStatus: StatusCode = StatusCodes.OK,
    responseContentType: ContentType = ContentTypes.`application/json`,
    state: SessionState.SessionState = SessionState.CONTINUE,
    rejection: Option[Rejection] = None,
    processorData: Map[String, Any] = Map.empty,
    startTime: Long = System.currentTimeMillis(),
    endTime: Option[Long] = None,
    cursor: AtomicInteger = new AtomicInteger(-1),
    pipeline: Seq[Processor] = Seq.empty
  ): Session =
    new Session(
      requestBody,
      headersFromSeq(requestHeaders),
      responseBody,
      headersFromSeq(responseHeaders),
      responseSource,
      responseStatus,
      responseContentType,
      state,
      rejection,
      processorData,
      startTime,
      endTime,
      cursor,
      pipeline
    )
}
