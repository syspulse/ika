package io.syspulse.ika.processor

object ResponseSource {
  type Source = Int

  val LOCAL: Source = 0
  val CACHE: Source = 1
  val REMOTE: Source = 2
}

