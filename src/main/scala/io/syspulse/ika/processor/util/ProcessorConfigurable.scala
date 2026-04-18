package io.syspulse.ika.processor.util

import com.typesafe.config.{Config => TypesafeConfig}
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext

import io.syspulse.ika.processor.Processor

/**
 * A processor (or processor group) that can be built from config subsection.
 *
 * Each subsection MUST include `type = "<processor-type>"`.
 * A builder may return multiple processors (e.g. http => HttpConfig + HttpClient).
 */
trait ProcessorConfigurable {
  def tpe: String
  def fromConfig(id: String, cfg: TypesafeConfig)(implicit ec: ExecutionContext, actorSystem: ActorSystem): Seq[Processor]
}

