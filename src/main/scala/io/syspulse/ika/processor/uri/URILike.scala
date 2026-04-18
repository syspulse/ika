package io.syspulse.ika.processor.uri

import scala.util.Try

trait URILike {
  val PREFIX_SEP: String = "://"
  
  protected final def splitUrlOps(uri: String): (String, Map[String, String]) = {
    uri.split("[\\?&]").toList match {
      case url :: Nil =>
        (url, Map.empty)
      case url :: tail =>
        val vars = tail.flatMap(_.split("=").toList match {
          case k :: v :: Nil => Some(k -> v)
          case _             => None
        }).toMap
        (url, vars)
      case _ =>
        ("", Map.empty)
    }
  }

  protected final def parseLong(s: String, default: Long): Long =
    Try(s.trim.toLong).getOrElse(default)
}

