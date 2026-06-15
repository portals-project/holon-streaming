package holon.utils

import java.net.URI
import java.net.{HttpURLConnection, URL}

/** Blocks until the distributed run should start: either HTTP (`START_GATE_URL`) or Firestore. */
object StartGate {

  private val StartGateUrlEnv = "START_GATE_URL"

  /** Polls `GET ${START_GATE_URL}/ready` until 200, or Firestore when the env is unset.
   *
   *  When `producerId` is supplied and HTTP mode is in use, the producer first POSTs
   *  `${START_GATE_URL}/announce?id=$producerId` (retried until accepted) so the gate can
   *  auto-flip itself once it has heard from `EXPECTED_PRODUCERS` peers. This removes the
   *  need for an external `/start` POST.
   */
  def waitUntilStarted(
      logger: Logger,
      waitMessage: => String,
      sleepMs: Long = 1000L,
      producerId: Option[Int] = None
  ): Unit =
    sys.env.get(StartGateUrlEnv).map(_.trim).filter(_.nonEmpty) match {
      case Some(baseUrl) =>
        producerId.foreach(id => announceReady(baseUrl, id, logger, sleepMs))
        waitHttp(baseUrl, logger, waitMessage, sleepMs)
      case None =>
        waitFirestore(logger, waitMessage, sleepMs)
    }

  private def joinPath(baseUrl: String, path: String): String = {
    val trimmed = if (baseUrl.endsWith("/")) baseUrl.dropRight(1) else baseUrl
    s"$trimmed/$path"
  }

  /** POST `${baseUrl}/announce?id=$producerId`, retrying on failure until a 2xx response. */
  private def announceReady(baseUrl: String, producerId: Int, logger: Logger, sleepMs: Long): Unit = {
    val uri    = URI.create(joinPath(baseUrl, s"announce?id=$producerId"))
    while (true) {
      try {
        val code = sendHttpRequest(uri.toString, "POST")
        if (code >= 200 && code < 300) {
          logger.info(s"Producer $producerId announced ready to start gate")
          return
        }
        logger.warn(s"Start gate /announce returned status $code; retrying")
      } catch {
        case e: Exception =>
          logger.warn(s"Start gate /announce failed: ${e.getMessage}; retrying")
      }
      Thread.sleep(sleepMs)
    }
  }

  private def waitHttp(baseUrl: String, logger: Logger, waitMessage: => String, sleepMs: Long): Unit = {
    val uri    = URI.create(joinPath(baseUrl, "ready"))
    while (true) {
      logger.info(waitMessage)
      try {
        val code = sendHttpRequest(uri.toString, "GET")
        if (code == 200) return
      } catch {
        case e: Exception =>
          logger.warn(s"Start gate HTTP poll failed: ${e.getMessage}")
      }
      Thread.sleep(sleepMs)
    }
  }

  private def sendHttpRequest(url: String, method: String): Int = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    conn.setConnectTimeout(5000)
    conn.setReadTimeout(10000)
    conn.setDoInput(true)
    if (method == "POST") conn.setDoOutput(true)
    try conn.getResponseCode
    finally conn.disconnect()
  }

  private def waitFirestore(logger: Logger, waitMessage: => String, sleepMs: Long): Unit = {
    import holon.streaming.cloud.FirestoreClient
    while (!FirestoreClient.isStartFlagSet) {
      logger.info(waitMessage)
      Thread.sleep(sleepMs)
    }
  }

}
