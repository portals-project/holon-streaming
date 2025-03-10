package holon.backend

import holon.*
import holon.example.CRDT.*
import holon.example.nexmark.Config.*
import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}
import upickle.default.writeBinary

/**
 * A case class for broadcasting the local window state.
 * It includes both the vector clock and the current window's counter.
 */
case class WindowState(
                        partitionId: Int,
                        vectorClock: Array[Long],
                        windowMap: scala.collection.mutable.Map[Long, (GCounter, Boolean)]
                      )

class WindowedRecordProcFun(partition: Int) extends ProcFun {
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")

  // (for logging) Counter for processed windows.
  private var windowCount: Int = 0

  // The vector clock holds the highest event-time seen from each partition.
  private val vectorClock: Array[Long] = Array.fill(KAFKA_N_PARTITIONS)(0L)

  // Define a mutable map with the window as the key and the (GCounter, Boolean) as the value.
  private val windowMap = scala.collection.mutable.Map.empty[Long, (GCounter, Boolean)]

  // Helper function to convert any object to Array[Byte] using its string representation.
  private def asBytes(x: Any): Array[Byte] = x.toString.getBytes("UTF-8")

  override def process(
                        outputFunction: (Byte, LogProducerRecords) => Unit,
                        chn: Byte,
                        recs: LogConsumerRecords
                      ): Unit = {
    chn match {
      case CHN_NEXMARK =>
        // Process incoming events from the Nexmark stream.
        for (rec <- recs) {
          val event = Nexmark.deserialize(rec._2).event
          event match {
            case bid: Nexmark.Events.Bid =>
              val eventTimestamp: Long = bid.dateTime
              // Update our own element in the vector clock.
              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)
//              logger.info(s"[window:$windowCount | partition:$partition] Updated vectorClock: ${vectorClock.mkString("Array(", ", ", ")")}")

              // Determine the window for this event.
              val window = defineWindow(eventTimestamp)

              // Initialize window counter if not present; otherwise, increment.
              if (!windowMap.contains(window))
                windowMap(window) = (GCounter.empty, false)
              else
                windowMap(window) = (windowMap(window)._1.increment(addr, 1L), false)

              // Determine the last window we can close, if value is -1, we can't close any window.
              if ((defineWindow(vectorClock.min) - 1) >= 0) {
                val passedWindow = defineWindow(vectorClock.min) - 1
                logger.info(s"[window:$windowCount | partition:$partition] Passed window: $passedWindow")

                // If the window exists and hasn't been processed, process (close) it.
                if (windowMap.contains(passedWindow) && !windowMap(passedWindow)._2) {
                  windowMap(passedWindow) = (windowMap(passedWindow)._1, true)
                  logger.info(s"[window:$windowCount | partition:$partition] Closing window: $passedWindow")
                  windowCount += 1

                  // Emit the current window's GCounter value.
                  // We convert both key (partition) and value (counter value) to Array[Byte].
                  outputFunction(CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(windowMap(passedWindow)._1.value))))
                }
              }
            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }
      case CHN_BROADCAST =>
        // Merge state received from other nodes.
        logger.info(s"[window:$windowCount | partition:$partition] Merging state from other partitions")
        for (rec <- recs) {
          // Assuming rec._2 is already a WindowState.
          val receivedState = rec._2.asInstanceOf[WindowState]
          val receivedPartition = receivedState.partitionId
          val receivedVectorClock = receivedState.vectorClock
          val receivedWindowMap = receivedState.windowMap

          // Merge window maps element-wise.
          for ((k, v) <- receivedWindowMap) {
            if windowMap.contains(k) then
            windowMap(k) = (windowMap(k)._1.merge(v._1), windowMap(k)._2)
          }

          // Merge vector clocks element-wise.
          for (i <- 0 until KAFKA_N_PARTITIONS) {
            vectorClock(i) = math.max(vectorClock(i), receivedVectorClock(i))
          }

          logger.info(s"[partition:$partition] Merging state from partition $receivedPartition: $receivedState")
        }

      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

//    var test = GCounter.empty
//    test = test.increment(addr, 1L)
//    outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(partition), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, test))))

    // Broadcast the current local state.
    val stateToBroadcast = WindowState(partition, vectorClock, windowMap)
    outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(partition), asBytes(stateToBroadcast))))
    logger.info(s"[window:$windowCount | partition:$partition] Broadcasting state: $stateToBroadcast")
  }

  // Returns the window index given an event time.
  override def defineWindow(eventTime: Long): Long = {
    val windowDuration: Long = 1000L
    if (eventTime % windowDuration == 0) eventTime / windowDuration
    else (eventTime / windowDuration) + 1
  }

  override def snapshot(): Array[Byte] = {
    // TODO: implement snapshot
    asBytes(0)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    // Restore not implemented.
  }
}
