package holon.backend

import holon.*
import holon.example.CRDT.*
import holon.example.nexmark.Config.*
import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.{GCounter, PNCounter, SelfUniqueAddress}
import upickle.default.{readwriter, macroRW, ReadWriter, writeBinary, readBinary}

// TODO: Examine the custom ReadWriter.
// Provide an implicit ReadWriter for GCounter using the CRDT functions.
// This converts a GCounter to its binary representation with a manifest and back.
implicit val gcounterRW: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
  gc => CRDT.crdtToBinaryWithManifest("GCounter", gc),
  bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
)

// Provide an implicit ReadWriter for mutable maps by converting them to immutable Maps.
implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[scala.collection.mutable.Map[K, V]] =
  readwriter[Map[K, V]].bimap[scala.collection.mutable.Map[K, V]](
    _.toMap,
    m => scala.collection.mutable.Map.empty[K, V] ++ m
  )

/**
 * A case class for broadcasting the local window state.
 * It includes both the vector clock and the current window's counter.
 */
case class WindowState(
                        partition: Int,
                        vectorClock: Array[Long],
                        windowMap: scala.collection.mutable.Map[Long, (GCounter, Boolean)]
                      )
object WindowState {
  implicit val rw: ReadWriter[WindowState] = macroRW
}

case class OutputState(
                        partition: Int,
                        window: Long,
                        value: BigInt
                      )
object OutputState {
  implicit val rw: ReadWriter[OutputState] = macroRW
}

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

              // Determine the window for this event.
              val window = defineWindow(eventTimestamp)

              // Initialize window counter if not present; otherwise, increment.
              if (!windowMap.contains(window))
                windowMap(window) = (GCounter.empty, false)
              else
                windowMap(window) = (windowMap(window)._1.increment(addr, 1L), false)

              // Determine the last window we can close.
              if ((defineWindow(vectorClock.min) - 1) >= 0) {
                val passedWindow = defineWindow(vectorClock.min) - 1
//                logger.info(s"[window:$windowCount | partition:$partition] Passed window: $passedWindow")
                if (windowMap.contains(passedWindow) && !windowMap(passedWindow)._2) {
                  windowMap(passedWindow) = (windowMap(passedWindow)._1, true)
//                  logger.info(s"[window:$windowCount | partition:$partition] Closing window: $passedWindow")
                  windowCount += 1

                  val outputState = OutputState(partition, passedWindow, windowMap(passedWindow)._1.value)
                  // Emit the current window's GCounter value.
                  outputFunction(CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(outputState))))
                }
              }
            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }
      case CHN_BROADCAST =>
//          logger.info(s"[window:$windowCount | partition:$partition] Merging state from other partitions")
          // Merge state received from other nodes.
          //        logger.info(s"[window:$windowCount | partition:$partition] Merging state from other partitions")
          for (rec <- recs) {
            // Deserialize the received state using upickle.
            val receivedState = readBinary[WindowState](rec._2)
            val receivedPartition = receivedState.partition
            val receivedVectorClock = receivedState.vectorClock
            val receivedWindowMap = receivedState.windowMap

//            logger.info(s"[partition:$partition] Received state from partition $receivedPartition: ${receivedVectorClock.mkString("Array(", ", ", ")")}")
//           Merge window maps element-wise.
            for ((k, v) <- receivedWindowMap) {
              if (windowMap.contains(k))
                windowMap(k) = (windowMap(k)._1.merge(v._1), windowMap(k)._2)
//                logger.info(s"[partition:$partition] Merged window $k with new value: ${windowMap(k)._1.value} merged with ${v._1},")
              else
                windowMap(k) = v
            }

            // Merge vector clocks element-wise.
            for (i <- 0 until KAFKA_N_PARTITIONS) {
//              logger.info(s"[partition:$partition] Merging vector clocksMerging vector clocks, comparing $i: ${vectorClock(i)} and ${receivedVectorClock(i)}")
              vectorClock(i) = math.max(vectorClock(i), receivedVectorClock(i))
            }

//            logger.info(s"[partition:$partition] Updated vector clock: ${vectorClock.mkString("Array(", ", ", ")")}")
        }
      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // Broadcast the current local state.
    val stateToBroadcast = WindowState(partition, vectorClock, windowMap)
    // TODO: Why is the partition hardcoded to 0?
    outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(stateToBroadcast))))

//    logger.info(s"[window:$windowCount | partition:$partition] Broadcasting state")
  }

  override def defineWindow(eventTime: Long): Long = {
    // windowDuration at 1000L results in ~90 bids per window.
    // windowDuration at 2000L results in ~180 bids per window.
    // It takes about 11,1 milliseconds to process 1 bid.
    val windowDuration: Long = 3000L
    if (eventTime % windowDuration == 0) eventTime / windowDuration
    else (eventTime / windowDuration) + 1
  }

  override def snapshot(): Array[Byte] = {
    // Example: snapshot the current vector clock.
    writeBinary(vectorClock)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    // Restore logic (if needed) can be implemented here.
  }
}
