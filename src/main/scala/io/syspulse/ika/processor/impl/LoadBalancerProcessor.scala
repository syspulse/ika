package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import java.util.concurrent.atomic.AtomicInteger

import io.syspulse.ika.processor.{RequestProcessor, Session}

/**
 * Load balancing strategy
 */
sealed trait LoadBalanceStrategy {
  def selectDestination(destinations: Seq[String], requestId: String): String
}

/**
 * Round-robin load balancing
 */
class RoundRobinStrategy extends LoadBalanceStrategy {
  private val counter = new AtomicInteger(0)

  def selectDestination(destinations: Seq[String], requestId: String): String = {
    val index = counter.getAndIncrement() % destinations.size
    destinations(math.abs(index))
  }
}

/**
 * Sticky load balancing - always use the first destination
 */
class StickyStrategy extends LoadBalanceStrategy {
  def selectDestination(destinations: Seq[String], requestId: String): String = {
    destinations.head
  }
}

/**
 * Random load balancing
 */
class RandomStrategy extends LoadBalanceStrategy {
  def selectDestination(destinations: Seq[String], requestId: String): String = {
    destinations(util.Random.nextInt(destinations.size))
  }
}

/**
 * Hash-based sticky load balancing - hash the request to select destination
 */
class HashStickyStrategy extends LoadBalanceStrategy {
  def selectDestination(destinations: Seq[String], requestId: String): String = {
    val hash = math.abs(requestId.hashCode)
    val index = hash % destinations.size
    destinations(index)
  }
}

/**
 * LoadBalancerProcessor selects a destination from a pool and sets it on the session.
 *
 * Features:
 * - Multiple load balancing strategies (round-robin, sticky, random, hash)
 * - Pool filtering: If session has a "pool" field set (e.g., by AIRouterProcessor),
 *   only destinations with matching pool tag are used
 * - Pool configuration format: "pool_name:http://host:port" or just "http://host:port"
 *
 * Example pool:
 * ```
 * Seq(
 *   "openai:https://api.openai.com/v1",
 *   "openai:https://api.openai.com/v2",
 *   "anthropic:https://api.anthropic.com/v1"
 * )
 * ```
 *
 * If session.pool = "openai", only the first two destinations are considered.
 */
class LoadBalancerProcessor(
  allDestinations: Seq[String],
  strategy: LoadBalanceStrategy = new RoundRobinStrategy
)(implicit ec: ExecutionContext) extends RequestProcessor {

  private val log = Logger(s"${name}")

  // Parse destinations into (poolName, url) tuples
  private val parsedDestinations: Seq[(Option[String], String)] = allDestinations.map { dest =>
    dest.split(":", 2).toList match {
      case poolName :: url :: Nil if url.startsWith("http") =>
        (Some(poolName), s"${poolName}:${url}") // Keep original format
      case _ =>
        (None, dest) // No pool tag
    }
  }

  def name: String = "LoadBalancer"

  /**
   * Filter destinations by pool name if session specifies a pool
   */
  private def filterByPool(poolName: Option[String]): Seq[String] = {
    poolName match {
      case Some(pool) =>
        val filtered = parsedDestinations.filter {
          case (Some(p), url) => p == pool
          case _ => false
        }.map(_._2)

        if (filtered.isEmpty) {
          log.warn(s"No destinations found for pool: $pool. Using all destinations.")
          allDestinations
        } else {
          filtered
        }

      case None =>
        // No pool specified - use all destinations
        allDestinations
    }
  }

  def processRequest(session: Session): Future[Session] = {
    // Check if cache hit - skip load balancing if response already cached
    session.getData[Boolean]("fromCache") match {
      case Some(true) =>
        log.debug("Response from cache, skipping load balancing")
        return Future.successful(session)
      case _ => // Continue
    }

    val poolName = session.getData[String]("pool")
    val availableDestinations = filterByPool(poolName)

    if (availableDestinations.isEmpty) {
      log.error(s"No destinations available for pool: ${poolName.getOrElse("default")}")
      return Future.successful(
        session.reject(
          code = -32603,
          message = s"No destinations available for pool: ${poolName.getOrElse("default")}",
          processorName = name
        )
      )
    }

    // Use request body as request ID for hash-based strategies
    val requestId = session.requestBody.take(100)
    val selectedDestination = strategy.selectDestination(availableDestinations, requestId)

    log.info(s"Selected destination: $selectedDestination (pool: ${poolName.getOrElse("default")}, strategy: ${strategy.getClass.getSimpleName})")

    Future.successful(session.putData("destination", selectedDestination))
  }
}

object LoadBalancerProcessor {
  def roundRobin(destinations: Seq[String])(implicit ec: ExecutionContext): LoadBalancerProcessor = {
    new LoadBalancerProcessor(destinations, new RoundRobinStrategy)
  }

  def sticky(destinations: Seq[String])(implicit ec: ExecutionContext): LoadBalancerProcessor = {
    new LoadBalancerProcessor(destinations, new StickyStrategy)
  }

  def random(destinations: Seq[String])(implicit ec: ExecutionContext): LoadBalancerProcessor = {
    new LoadBalancerProcessor(destinations, new RandomStrategy)
  }

  def hashSticky(destinations: Seq[String])(implicit ec: ExecutionContext): LoadBalancerProcessor = {
    new LoadBalancerProcessor(destinations, new HashStickyStrategy)
  }
}
