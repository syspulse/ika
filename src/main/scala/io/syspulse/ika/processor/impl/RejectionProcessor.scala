package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger

import io.syspulse.ika.processor.{Processor, Session, Rejection}
import io.syspulse.ika.store.ProxyData

/**
 * RejectionProcessor converts rejections into formatted error responses.
 *
 * This is an abstract processor that should be placed at the end of the pipeline
 * to handle any rejections that occurred during processing.
 *
 * Different implementations can produce different error formats and HTTP status codes:
 * - JsonRpcRejectionProcessor: JSON-RPC 2.0 format
 * - RestApiRejectionProcessor: REST API format with appropriate HTTP status codes
 * - CustomRejectionProcessor: User-defined format
 *
 * The processor sets:
 * - responseBody: Formatted error message
 * - processorData("httpStatusCode"): HTTP status code for the response
 *
 * Note: RejectionProcessor extends Processor directly (not ResponseProcessor) because
 * it needs to process rejected sessions, while ResponseProcessor short-circuits on rejection.
 */
abstract class RejectionProcessor(implicit ec: ExecutionContext) extends Processor {

  private val log = Logger(s"${name}")

  def name: String

  /**
   * Convert rejection to response body
   */
  def formatRejection(rejection: Rejection, session: Session): String

  /**
   * Determine HTTP status code from rejection
   */
  def getHttpStatusCode(rejection: Rejection, session: Session): Int

  /**
   * Process the session - converts rejection to error response
   */
  def process(session: Session): Future[Session] = {
    session.rejection match {
      case Some(rejection) =>
        log.debug(s"Converting rejection: ${rejection}")

        val responseBody = formatRejection(rejection, session)
        val httpStatusCode = getHttpStatusCode(rejection, session)

        log.info(s"Rejection response: HTTP ${httpStatusCode}, processor: ${rejection.processorName}, code: ${rejection.code}")

        Future.successful(
          session
            .withResponse(responseBody, ProxyData.LOCAL)
            .putData("httpStatusCode", httpStatusCode)
        )

      case None =>
        // No rejection, pass through
        Future.successful(session)
    }
  }
}

/**
 * JsonRpcRejectionProcessor converts rejections to JSON-RPC 2.0 error format.
 *
 * Output format:
 * ```json
 * {
 *   "jsonrpc": "2.0",
 *   "error": {
 *     "code": -32603,
 *     "message": "Internal error",
 *     "data": {
 *       "processor": "HttpClient",
 *       "details": "Connection timeout"
 *     }
 *   },
 *   "id": null
 * }
 * ```
 *
 * HTTP status codes:
 * - Always returns 200 (JSON-RPC errors are not HTTP errors)
 * - Unless configured otherwise
 */
class JsonRpcRejectionProcessor(
  httpStatusCode: Int = 200,  // JSON-RPC typically returns 200 even for errors
  includeProcessor: Boolean = true,
  includeDetails: Boolean = true
)(implicit ec: ExecutionContext) extends RejectionProcessor {

  def name: String = "JsonRpcRejection"

  def formatRejection(rejection: Rejection, session: Session): String = {
    val dataFields = Seq(
      if (includeProcessor) Some(s""""processor": "${rejection.processorName}"""") else None,
      if (includeDetails && rejection.details.isDefined) Some(s""""details": "${rejection.details.get}"""") else None
    ).flatten

    val dataJson = if (dataFields.nonEmpty) {
      s""", "data": { ${dataFields.mkString(", ")} }"""
    } else {
      ""
    }

    s"""{"jsonrpc": "2.0", "error": {"code": ${rejection.code}, "message": "${rejection.message}"${dataJson}}, "id": null}"""
  }

  def getHttpStatusCode(rejection: Rejection, session: Session): Int = {
    httpStatusCode
  }
}

/**
 * RestApiRejectionProcessor converts rejections to REST API JSON format.
 *
 * Output format:
 * ```json
 * {
 *   "error": {
 *     "code": "INTERNAL_ERROR",
 *     "message": "Internal error",
 *     "processor": "HttpClient",
 *     "details": "Connection timeout"
 *   }
 * }
 * ```
 *
 * HTTP status codes:
 * - Maps rejection codes to HTTP status codes
 * - Default: 500 (Internal Server Error)
 * - 400-499 rejection codes → Same HTTP status
 * - Other codes → configurable default
 */
class RestApiRejectionProcessor(
  defaultHttpStatus: Int = 500,
  includeProcessor: Boolean = true,
  includeDetails: Boolean = true
)(implicit ec: ExecutionContext) extends RejectionProcessor {

  def name: String = "RestApiRejection"

  def formatRejection(rejection: Rejection, session: Session): String = {
    val fields = Seq(
      Some(s""""code": "${rejection.code}""""),
      Some(s""""message": "${rejection.message}""""),
      if (includeProcessor) Some(s""""processor": "${rejection.processorName}"""") else None,
      if (includeDetails && rejection.details.isDefined) Some(s""""details": "${rejection.details.get}"""") else None
    ).flatten

    s"""{"error": { ${fields.mkString(", ")} }}"""
  }

  def getHttpStatusCode(rejection: Rejection, session: Session): Int = {
    // Map rejection codes to HTTP status codes
    rejection.code match {
      case code if code >= 400 && code < 600 => code  // HTTP status codes
      case -32700 => 400  // Parse error
      case -32600 => 400  // Invalid request
      case -32601 => 404  // Method not found
      case -32602 => 400  // Invalid params
      case -32603 => 500  // Internal error
      case _ => defaultHttpStatus
    }
  }
}

/**
 * CustomRejectionProcessor allows custom rejection formatting.
 *
 * Provides a flexible way to create custom error formats without creating a new class.
 */
class CustomRejectionProcessor(
  formatter: (Rejection, Session) => String,
  statusCodeMapper: (Rejection, Session) => Int
)(implicit ec: ExecutionContext) extends RejectionProcessor {

  def name: String = "CustomRejection"

  def formatRejection(rejection: Rejection, session: Session): String = {
    formatter(rejection, session)
  }

  def getHttpStatusCode(rejection: Rejection, session: Session): Int = {
    statusCodeMapper(rejection, session)
  }
}

object RejectionProcessor {
  /**
   * Create JSON-RPC rejection processor (default for Web3)
   */
  def jsonRpc(
    httpStatusCode: Int = 200,
    includeProcessor: Boolean = true,
    includeDetails: Boolean = true
  )(implicit ec: ExecutionContext): JsonRpcRejectionProcessor = {
    new JsonRpcRejectionProcessor(httpStatusCode, includeProcessor, includeDetails)
  }

  /**
   * Create REST API rejection processor
   */
  def restApi(
    defaultHttpStatus: Int = 500,
    includeProcessor: Boolean = true,
    includeDetails: Boolean = true
  )(implicit ec: ExecutionContext): RestApiRejectionProcessor = {
    new RestApiRejectionProcessor(defaultHttpStatus, includeProcessor, includeDetails)
  }

  /**
   * Create custom rejection processor
   */
  def custom(
    formatter: (Rejection, Session) => String,
    statusCodeMapper: (Rejection, Session) => Int
  )(implicit ec: ExecutionContext): CustomRejectionProcessor = {
    new CustomRejectionProcessor(formatter, statusCodeMapper)
  }
}
