package io.syspulse.ika.processor.impl

import scala.concurrent.{Future, ExecutionContext}
import com.typesafe.scalalogging.Logger
import java.util.concurrent.atomic.AtomicInteger

import io.syspulse.ika.processor.{Session, Processor}
import io.syspulse.ika.processor.uri.PoolURI
import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem
import io.syspulse.ika.processor.util.ProcessorConfigurable
import scala.jdk.CollectionConverters._

/**
 * Load balancing strategy
 */
sealed trait PoolStrategy {
  def strategyName: String
  override def toString: String = strategyName

  /** Pick the starting index for this request. */
  def startIndex(destinations: Seq[String], requestId: String): Int

  /** Called when a destination succeeds (non-rejected downstream result). */
  def onSuccess(idx: Int, poolSize: Int): Unit = {}

  /**
   * Called when the pool is exhausted for this request (we tried the whole ring and failed).
   * Used by sticky to advance its "current" start point.
   */
  def onExhausted(lastTriedIdx: Int, poolSize: Int): Unit = {}
}

/**
 * Round-robin load balancing
 */
final class RoundRobinStrategy extends PoolStrategy {
  private val counter = new AtomicInteger(0)

  override val strategyName: String = PoolProcessor.RoundRobin.name

  override def startIndex(destinations: Seq[String], requestId: String): Int =
    math.abs(counter.getAndIncrement()) % destinations.size
}

/**
 * Sticky load balancing:
 * - Tries to stay on the last successful destination.
 * - If the pool is exhausted in errors, the last tried destination becomes the start for the next request.
 */
final class StickyStrategy extends PoolStrategy {
  override val strategyName: String = PoolProcessor.Sticky.name
  private val current = new AtomicInteger(0)

  override def startIndex(destinations: Seq[String], requestId: String): Int =
    math.abs(current.get()) % destinations.size

  override def onSuccess(idx: Int, poolSize: Int): Unit =
    current.set(math.abs(idx) % poolSize)

  override def onExhausted(lastTriedIdx: Int, poolSize: Int): Unit =
    current.set(math.abs(lastTriedIdx) % poolSize)
}

/**
 * Load-balancing:
 * - Always starts with the next destination after the last successful one.
 * - On errors it still walks the whole pool (same as sticky) looking for a working destination.
 */
final class LoadBalancerStrategy extends PoolStrategy {
  override val strategyName: String = PoolProcessor.LoadBalancer.name
  private val lastSuccess = new AtomicInteger(-1)

  override def startIndex(destinations: Seq[String], requestId: String): Int = {
    val n = destinations.size
    val ls = lastSuccess.get()
    if (ls < 0) 0 else (math.abs(ls) + 1) % n
  }

  override def onSuccess(idx: Int, poolSize: Int): Unit =
    lastSuccess.set(math.abs(idx) % poolSize)
}

/**
 * Random load balancing
 */
final class RandomStrategy extends PoolStrategy {
  override val strategyName: String = PoolProcessor.Random.name
  override def startIndex(destinations: Seq[String], requestId: String): Int =
    util.Random.nextInt(destinations.size)
}

/**
 * Hash-based sticky load balancing - hash the request to select destination
 */
final class HashStickyStrategy extends PoolStrategy {
  override val strategyName: String = PoolProcessor.Hash.name
  override def startIndex(destinations: Seq[String], requestId: String): Int = {
    val hash = math.abs(requestId.hashCode)
    hash % destinations.size
  }
}

/**
 * PoolProcessor selects a destination from a pool and sets it on the session.
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
class PoolProcessor(
  allDestinations: Seq[String],
  strategy: PoolStrategy = new RoundRobinStrategy
)(implicit ec: ExecutionContext) extends Processor {

  private val log = Logger(s"${name}")

  private case class Dest(poolTag: Option[String], label: Option[String], url: String, raw: String)

  // Supported destination formats:
  // - "poolTag:http://..."  -> poolTag used for filtering; url used for actual destination
  // - "name=http://..."     -> name is a label only (NOT used for pool filtering); url used for actual destination
  // - "http://..."          -> plain url
  private val parsedDestinations: Seq[Dest] = allDestinations.map { d0 =>
    val dest = d0.trim
    dest.split(":", 2).toList match {
      case tag :: url :: Nil if tag.nonEmpty && url.startsWith("http") =>
        Dest(poolTag = Some(tag), label = None, url = url, raw = dest)
      case _ =>
        dest.split("=", 2).toList match {
          case name :: url :: Nil if name.trim.nonEmpty && url.trim.startsWith("http") =>
            Dest(poolTag = None, label = Some(name.trim), url = url.trim, raw = dest)
          case _ =>
            Dest(poolTag = None, label = None, url = dest, raw = dest)
        }
    }
  }

  def name: String = "Pool"

  override def toString: String = s"${name}($strategy,${allDestinations})"

  /**
   * Filter destinations by pool name if session specifies a pool
   */
  private def filterByPool(poolName: Option[String]): Seq[String] = {
    poolName match {
      case Some(pool) =>
        val filtered = parsedDestinations
          .filter(d => d.poolTag.contains(pool))
          .map(_.url)

        if (filtered.isEmpty) {
          log.warn(s"No destinations found for pool: $pool. Using all destinations.")
          parsedDestinations.map(_.url)
        } else {
          filtered
        }

      case None =>
        // No pool specified - use all destinations
        parsedDestinations.map(_.url)
    }
  }

  override def process(session: Session): Future[Session] = {
    if (session.isRejected) return Future.successful(session)
    if (session.shouldReturn) return Future.successful(session)

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
    val requestId = session.requestBody.take(100).utf8String

    // The cursor currently points at this processor. Downstream starts at cursor+1.
    val baseCursor = session.cursor.get()
    val startIdx = strategy.startIndex(availableDestinations, requestId)

    def resetForAttempt(s: Session): Session =
      s.withCursor(baseCursor)
        .copy(
          state = io.syspulse.ika.processor.SessionState.CONTINUE,
          rejection = None,
          responseBody = None,
          responseHeaderMap = Map.empty,
          responseSource = io.syspulse.ika.processor.ResponseSource.LOCAL
        )
        // RetryProcessor must restart its counters for a new destination
        .removeData("retry")
        .removeData("maxRetry")
        .removeData("errorReason")

    def attempt(idx: Int, start: Int): Future[Session] = {
      val dest = availableDestinations(idx)
      log.info(
        s"Selected destination: $dest (pool: ${poolName.getOrElse("default")}, strategy: ${strategy.strategyName})"
      )

      val attemptSession = resetForAttempt(session.putData("destination", dest))

      next(attemptSession).flatMap { result =>
        if (!result.isRejected) {
          strategy.onSuccess(idx, availableDestinations.size)
          Future.successful(result)
        } else {
          val nextIdx = (idx + 1) % availableDestinations.size
          if (nextIdx == start) {
            // exhausted the ring
            strategy.onExhausted(idx, availableDestinations.size)
            Future.successful(result)
          } else {
            attempt(nextIdx, start)
          }
        }
      }
    }

    // If there is no downstream processor, behave like the old PoolProcessor (just set destination).
    if (session.pipeline.lift(baseCursor + 1).isEmpty) {
      val dest = availableDestinations(startIdx)
      Future.successful(session.putData("destination", dest))
    } else {
      attempt(startIdx, startIdx)
    }
  }
}

object PoolProcessor extends ProcessorConfigurable {
  override val tpe: String = "pool"

  sealed trait StrategyConfigurable {
    def name: String
    def fromConfig(cfg: TypesafeConfig): PoolStrategy
  }

  object Sticky extends StrategyConfigurable {
    override val name: String = "sticky"
    override def fromConfig(cfg: TypesafeConfig): PoolStrategy = new StickyStrategy
  }

  object RoundRobin extends StrategyConfigurable {
    override val name: String = "rr"
    override def fromConfig(cfg: TypesafeConfig): PoolStrategy = new RoundRobinStrategy
  }

  object LoadBalancer extends StrategyConfigurable {
    override val name: String = "lb"
    override def fromConfig(cfg: TypesafeConfig): PoolStrategy = new LoadBalancerStrategy
  }

  object Random extends StrategyConfigurable {
    override val name: String = "random"
    override def fromConfig(cfg: TypesafeConfig): PoolStrategy = new RandomStrategy
  }

  object Hash extends StrategyConfigurable {
    override val name: String = "hash"
    override def fromConfig(cfg: TypesafeConfig): PoolStrategy = new HashStickyStrategy
  }

  private val strategies: Map[String, StrategyConfigurable] =
    Seq(
      Sticky, 
      RoundRobin, 
      LoadBalancer, 
      Random, 
      Hash)
    .map(s => s.name -> s).toMap ++
    Map(
      "round_robin" -> RoundRobin,
      "roundrobin" -> RoundRobin,
      "loadbalancer" -> LoadBalancer,      
    )

  def roundRobin(destinations: Seq[String])(implicit ec: ExecutionContext): PoolProcessor =
    new PoolProcessor(destinations, new RoundRobinStrategy)

  def loadBalancer(destinations: Seq[String])(implicit ec: ExecutionContext): PoolProcessor =
    new PoolProcessor(destinations, new LoadBalancerStrategy)

  def sticky(destinations: Seq[String])(implicit ec: ExecutionContext): PoolProcessor =
    new PoolProcessor(destinations, new StickyStrategy)

  def random(destinations: Seq[String])(implicit ec: ExecutionContext): PoolProcessor =
    new PoolProcessor(destinations, new RandomStrategy)

  def hashSticky(destinations: Seq[String])(implicit ec: ExecutionContext): PoolProcessor =
    new PoolProcessor(destinations, new HashStickyStrategy)

  /** Build from [[PoolURI]] and fallback destinations when the URI does not embed a list. */
  def fromUri(p: PoolURI, fallbackDestinations: Seq[String])(implicit ec: ExecutionContext): PoolProcessor = {
    val dest = p.destinations(fallbackDestinations)
    p.strategy.trim match {
      case "sticky" =>
        sticky(dest)
      case "roundrobin" | "round_robin" | "rr" =>
        roundRobin(dest)
      case "lb" | "loadbalancer" =>
        loadBalancer(dest)
      case "random" =>
        random(dest)
      case "hash" =>
        hashSticky(dest)
      case _ =>
        sticky(dest)
    }
  }

  override def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor] = {
    val strategyName = if (cfg.hasPath("strategy")) cfg.getString("strategy").toLowerCase.trim else "sticky"

    val destinations =
      if (cfg.hasPath("destinations")) cfg.getStringList("destinations").asScala.toSeq
      else Seq.empty

    // Keep raw strings as-is:
    // - "name=http://..." should remain a label form (NOT converted to "name:http://...")
    // - "pool:http://..." is a pool-tag form
    val dest = destinations.map(_.trim).filter(_.nonEmpty)

    val st = strategies.get(strategyName).map(_.fromConfig(cfg)).getOrElse(new StickyStrategy)
    Seq(new PoolProcessor(dest, st))
  }
}
