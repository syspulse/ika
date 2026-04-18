package io.syspulse.ika.processor

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

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
   * Call the next processor in the current session pipeline.
   * The session must have been initialized with `withPipeline(...)`.
   */
  final def next(session: Session)(implicit ec: ExecutionContext): Future[Session] = {
    session.nextProcessor match {
      case Some(p) =>
        p.process(session).recover { case ex: Exception =>
          session.reject(
            code = -32603,
            message = s"Internal processor error: ${ex.getMessage}",
            processorName = p.name,
            details = Some(ex.getClass.getSimpleName)
          )
        }
      case None    => Future.successful(session)
    }
  }

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
    } else if (session.shouldReturn) {
      Future.successful(session)
    } else {
      implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
      processRequest(session).flatMap { s =>
        if (s.isRejected || s.shouldReturn) Future.successful(s)
        else next(s)
      }
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
    } else if (session.shouldReturn) {
      Future.successful(session)
    } else {
      implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
      processResponse(session).flatMap { s =>
        if (s.isRejected || s.shouldReturn) Future.successful(s)
        else next(s)
      }
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
    if (session.isRejected || session.shouldReturn) {
      Future.successful(session)
    } else {
      import scala.concurrent.ExecutionContext.Implicits.global
      for {
        reqSession <- processRequest(session)
        downSession <- if (reqSession.isRejected || reqSession.shouldReturn) Future.successful(reqSession) else next(reqSession)
        respSession <- if (downSession.isRejected || downSession.shouldReturn) Future.successful(downSession) else processResponse(downSession)
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
