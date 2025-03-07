package holon.backend

import org.apache.pekko.cluster.ddata.{PNCounter, SelfUniqueAddress}
import upickle.default.*
import holon.*
import holon.example.nexmark.Config.*
import holon.example.{CRDT, Nexmark}
import holon.example.CRDT.*

// Define a custom ReadWriter for PNCounter that uses our CRDT serialization.
implicit val pncounterRW: ReadWriter[PNCounter] = readwriter[Array[Byte]].bimap[PNCounter](
  (p: PNCounter) => CRDT.crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, p),
  (bytes: Array[Byte]) => CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[PNCounter]
)

/**
 * A case class for broadcasting the local window state.
 * It includes both the vector clock and the current window's counter.
 */
case class WindowState(partitionId: Int, vectorClock: Array[Long], currentWindowCounter: PNCounter, currentWindowStart: Long, currentWindowEnd: Long, windowCount: Int)
object WindowState {
  implicit val rw: ReadWriter[WindowState] = macroRW
}

/**
 * This ProcFun implementation applies a tumbling window to incoming bid events.
 * Each window maintains a local PN counter for the number of bids received.
 * Each window also maintains a vector clock to track the event-time progress among all partitions.
 * The window boundaries are also aligned across partitions to keep every partition in sync with the increasing vector clock.
 */
class WindowedRecordProcFun(partition: Int) extends ProcFun {
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")

  // Tumbling window duration in milliseconds.
  // Large window for debugging purposes.
  private val windowDuration: Long = 1000000L

  // Window boundaries (based on event timestamps), should be aligned across partitions somehow.
  private var currentWindowStart: Long = -1L
  private var currentWindowEnd: Long = -1L

  // Local PN counter for partition.
  private var currentWindowCounter: PNCounter = PNCounter.empty

  // (for logging) Counter for processed windows.
  private var windowCount: Int = 0

  // The vector clock holds the highest event-time seen from each partition.
  private var vectorClock: Array[Long] = Array.fill(KAFKA_N_PARTITIONS)(0L)

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
              logger.info(s"[window:$windowCount | partition:$partition] Updated vectorClock: ${vectorClock.mkString("Array(", ", ", ")")}")

              // Initialize window boundaries on the first event.
              if (currentWindowStart == -1L) {
                currentWindowStart = 0L
                currentWindowEnd = currentWindowStart + windowDuration
                logger.info(s"[window:$windowCount | partition:$partition] First event with timestamp $eventTimestamp. Setting window to [$currentWindowStart, $currentWindowEnd)")
              }
              // If the event falls into the current window, update the counter.
              if (eventTimestamp >= currentWindowStart && eventTimestamp < currentWindowEnd) {
                currentWindowCounter = currentWindowCounter.increment(addr, 1L)
                logger.info(s"[window:$windowCount | partition:$partition] Incremented counter; new value: ${currentWindowCounter.value}")
              }

              // Else if the event is beyond the current window and all nodes have advanced, close the window.
              else if (vectorClock.min >= currentWindowEnd) {
                logger.info(s"[window:$windowCount | partition:$partition] Closing window. Current window counter: ${currentWindowCounter.value}")

                // Emit the current window counter's value.
                outputFunction(CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(currentWindowCounter.value))))

                // Reset the local window by setting value to 0.
                currentWindowCounter = currentWindowCounter.decrement(addr, currentWindowCounter.value)
                windowCount += 1

                // Update the window boundaries using the minimum value from the vector clock.
                val newStart = (vectorClock.min / windowDuration) * windowDuration
                currentWindowStart = newStart
                currentWindowEnd = currentWindowStart + windowDuration
                logger.info(s"[window:$windowCount | partition:$partition] New window boundaries: [$currentWindowStart, $currentWindowEnd) based on vectorClock.min=${vectorClock.min}")

                // Process the current event in the new window if applicable.
                if (eventTimestamp >= currentWindowStart && eventTimestamp < currentWindowEnd) {
                  currentWindowCounter = currentWindowCounter.increment(addr, 1L)
                }
              } else {
                logger.info(s"[window:$windowCount | partition:$partition] Event with timestamp $eventTimestamp does not fit in current window [$currentWindowStart, $currentWindowEnd) and cannot trigger window close since vectorClock.min=${vectorClock.min} < currentWindowEnd.")
              }
            case _ =>
              logger.debug(s"Ignored non-bid event: $event")
          }
        }

      case CHN_BROADCAST =>
        // Merge state received from other nodes.
        for (rec <- recs) {
          // Deserialize the broadcasted state as WindowState.
          val receivedState = readBinary[WindowState](rec._2)

          logger.info(s"[window:$windowCount | partition:$partition] Received broadcast from partition ${receivedState.partitionId} with state: $receivedState")

          // Merge vector clocks element-wise.
          for (i <- 0 until KAFKA_N_PARTITIONS) {
            vectorClock(i) = math.max(vectorClock(i), receivedState.vectorClock(i))
          }
          // Update window boundaries based on receivedState if data is ahead of local state.
          if (receivedState.currentWindowStart > currentWindowStart) {
            logger.info(s"[window:$windowCount | partition:$partition] Updating window boundaries based on received state")
            currentWindowStart = receivedState.currentWindowStart
            currentWindowEnd = receivedState.currentWindowEnd
            windowCount = receivedState.windowCount
          }

          // Merge the PN counter. PN counter merge preserves per-replica contributions, summing them.
          currentWindowCounter = currentWindowCounter.merge(receivedState.currentWindowCounter)
          logger.info(s"[window:$windowCount | partition:$partition] Merged broadcast state. Updated vectorClock.min=${vectorClock.min} and counter value=${receivedState.currentWindowCounter.value}")
        }

      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // Broadcast the current local state: both the vector clock and current window counter.
    val stateToBroadcast = WindowState(partition, vectorClock, currentWindowCounter, currentWindowStart, currentWindowEnd, windowCount)
    logger.info(s"[window:$windowCount | partition:$partition] Broadcasting state: $stateToBroadcast")
    outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(partition), writeBinary(stateToBroadcast))))
  }

  override def snapshot(): Array[Byte] = {
    // Snapshot the current window counter's value.
    writeBinary(currentWindowCounter.value)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    // Restore not implemented.
  }
}
