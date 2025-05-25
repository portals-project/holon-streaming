package holon.backend

import holon.*
import holon.Config.{CHN_INPUT, CHN_OUTPUT}
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}
import scala.collection.mutable

// Pass through
class Q0ProcessFun(partition: Int) extends ProcFun {
  private val logger = Logger("Q0ProcessFun")
  private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("Q0ProcessFun", "INFO")

  // Track throughput per second
  private val eventCounterPerSecond = mutable.Map.empty[Long, Long]

  // Track max log append time per second
  private val logAppendTimePerSecond = mutable.Map.empty[Long, Long]

  override def processInput(
                             outputFun: (Int, Byte, LogProducerRecords) => Unit,
                             chn: Byte,
                             recs: LogConsumerRecords
                           ): Unit = {
    val nowInSeconds = System.currentTimeMillis() / 1000
    if chn == CHN_INPUT then
      for record <- recs do
        // record._3 is the Kafka log append timestamp
        val logAppendTs = record._3

        val recordId = java.util.UUID.randomUUID()

        // Count events per second
        eventCounterPerSecond(nowInSeconds) =
          eventCounterPerSecond.getOrElse(nowInSeconds, 0L) + 1


        // Run this code 1 out of 100 times
        if scala.util.Random.nextInt(400) == 1 then
          // Log simulated latency (as in Q7)
          logger.info(s"[LagAppendInput] - event: $recordId, timestamp: $logAppendTs")
          //        outputLog.info(s"[LagAppendInput] - event: $recordId , timestamp: $logAppendTs")
          // Output the record wrapped in OutputState
          val out: OutputState = OutputState(partition, 0L, recordId.toString)
          outputFun(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](out))))


      // Periodically log throughput
      val currentSecond = System.currentTimeMillis() / 1000
      val expiredSeconds = eventCounterPerSecond.keys.filter(_ < currentSecond)

      for s <- expiredSeconds do
        logger.info(s"[Throughput] second: $s, eventCount: ${eventCounterPerSecond(s)}")
        //        outputLog.info(s"[Throughput] second: $s, eventCount: ${eventCounterPerSecond(s)}")
        eventCounterPerSecond -= s
        logAppendTimePerSecond -= s
  }

  override def snapshot(): Array[Byte] = Array.emptyByteArray

  override def restore(bytes: Array[Byte]): Unit = {}
}