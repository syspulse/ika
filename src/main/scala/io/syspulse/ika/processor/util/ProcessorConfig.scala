package io.syspulse.ika.processor.util

/**
 * PipelineProfile contains a common processor set configuration.
 *
 * These settings are used by processors in the pipeline and should not be
 * in the main Config class (which is generic for the proxy server).
 *
 * Configuration can come from:
 * - application.conf (Typesafe Config)
 * - Command-line arguments
 * - Processor URI parameters
 * - Defaults
 */
case class PipelineProfile(
  // HTTP Client
  timeout: Long = 10000L,           // Request timeout in milliseconds
  compress: String = "",             // Compression: "gzip,deflate" or ""
  threads: Int = 4,                  // Number of threads for executor

  // Retry
  retry: Int = 3,                    // Maximum retry attempts
  retryDelay: Long = 1000L,          // Delay between retries in milliseconds

  // Load Balancer
  laps: Int = 1,                     // Number of pool laps before giving up
  failback: Long = 10000L,           // Delay before retrying failed node

  // Throttle
  throttle: Long = 0L,               // Global throttle delay in milliseconds (0 = disabled)

  // Headers
  headers: Seq[String] = Seq()       // Additional headers to send
)

object PipelineProfile {
  /**
   * Default configuration
   */
  def default: PipelineProfile = PipelineProfile()

  /**
   * Configuration for Web3/RPC use case
   */
  def web3: PipelineProfile = PipelineProfile(
    timeout = 150L,
    retry = 3,
    retryDelay = 1000L,
    laps = 1,
    failback = 10000L,
    throttle = 0L,
    compress = ""
  )

  /**
   * Configuration for AI API use case
   */
  def ai: PipelineProfile = PipelineProfile(
    timeout = 30000L,    // AI APIs can be slower
    retry = 2,
    retryDelay = 2000L,
    laps = 1,
    failback = 10000L,
    throttle = 0L,
    compress = "gzip,deflate"
  )

  /**
   * Fast configuration for low-latency use cases
   */
  def fast: PipelineProfile = PipelineProfile(
    timeout = 1000L,
    retry = 1,
    retryDelay = 100L,
    laps = 1,
    failback = 5000L,
    throttle = 0L,
    compress = ""
  )
}
