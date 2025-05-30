package holon

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.turbo.TurboFilter
import ch.qos.logback.core.spi.FilterReply
import org.slf4j.Marker

import java.util.concurrent.atomic.AtomicInteger

class ThrottleFilter extends TurboFilter {
  // these get injected from logback.xml
  private var maxMessages: Long = 1000L
  private var intervalMs: Long = 60000L // 1 minute

  private val counter = new AtomicInteger(0)
  @volatile private var windowStart = System.currentTimeMillis()

  // setters for XML
  def setMaxMessages(m: Long): Unit = maxMessages = m
  def setIntervalMs(i: Long): Unit = intervalMs = i

  override def decide(marker: Marker,
                      logger: Logger,
                      level: Level,
                      message: String,
                      paramArray: Array[Object],
                      t: Throwable): FilterReply = {
    val now = System.currentTimeMillis()
    if (now - windowStart > intervalMs) {
      windowStart = now
      counter.set(0)
    }
    if (counter.incrementAndGet() <= maxMessages) FilterReply.NEUTRAL
    else FilterReply.DENY
  }
}
