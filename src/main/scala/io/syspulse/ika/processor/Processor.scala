package io.syspulse.ika.processor

import scala.concurrent.Future

/**
 * Base trait for all processors in the pipeline.
 * Processors are the fundamental building blocks that transform Sessions.
 */
trait Processor {
  /**
   * Process a session and return the modified session.
   * Processors should:
   * - Be stateless (all state in Session)
   * - Return Future for async operations
   * - Short-circuit if session is already rejected
   * - Use immutable Session updates
   */
  def process(session: Session): Future[Session]

  /**
   * Unique name for this processor (used in logging and rejection messages)
   */
  def name: String
}

/**
 * Processor that only processes the request phase (before calling downstream).
 * Use for: validation, routing, authentication, request transformation.
 */
trait RequestProcessor extends Processor {
  /**
   * Process the request phase.
   * Called before any downstream HTTP call.
   */
  def processRequest(session: Session): Future[Session]

  final def process(session: Session): Future[Session] = {
    if (session.isRejected) {
      Future.successful(session)
    } else {
      processRequest(session)
    }
  }
}

/**
 * Processor that only processes the response phase (after downstream returns).
 * Use for: response transformation, caching, logging.
 */
trait ResponseProcessor extends Processor {
  /**
   * Process the response phase.
   * Called after downstream HTTP call completes.
   * Session will have responseBody populated.
   */
  def processResponse(session: Session): Future[Session]

  final def process(session: Session): Future[Session] = {
    if (session.isRejected) {
      Future.successful(session)
    } else {
      processResponse(session)
    }
  }
}

/**
 * Processor that processes both request and response phases.
 * Use for: correlation, timing, request/response pairs.
 */
trait BidirectionalProcessor extends Processor {
  /**
   * Process the request phase
   */
  def processRequest(session: Session): Future[Session]

  /**
   * Process the response phase
   */
  def processResponse(session: Session): Future[Session]

  /**
   * Default implementation processes request then response.
   * Override process() if you need different behavior.
   */
  def process(session: Session): Future[Session] = {
    if (session.isRejected) {
      Future.successful(session)
    } else {
      import scala.concurrent.ExecutionContext.Implicits.global
      for {
        reqSession <- processRequest(session)
        respSession <- if (reqSession.isRejected) Future.successful(reqSession) else processResponse(reqSession)
      } yield respSession
    }
  }
}

/**
 * A processor that wraps another processor or pipeline and can intercept/modify behavior.
 * Use for: retry, timeout, circuit breaker patterns.
 */
trait WrapperProcessor extends Processor {
  /**
   * The wrapped processor or pipeline
   */
  def wrapped: Processor
}
