package holon.backend

import holon.*
import holon.example.CRDT._
import holon.example.nexmark.Config._
import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}
import upickle.default.{readwriter, macroRW, ReadWriter, writeBinary, readBinary}

// Custom ReadWriter for GCounter remains unchanged.
implicit val gcounterRW: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
  gc => CRDT.crdtToBinaryWithManifest("GCounter", gc),
  bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
)

// A generic implicit for mutable maps (if needed for other types)
implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[scala.collection.mutable.Map[K, V]] =
  readwriter[Map[K, V]].bimap[scala.collection.mutable.Map[K, V]](
    _.toMap,
    m => scala.collection.mutable.Map.empty[K, V] ++ m
  )

// *** New: Explicit implicit for the window map ***
// This ensures that a mutable Map[String, (GCounter, Boolean)] is encoded as a dictionary.
implicit val windowMapRW: ReadWriter[scala.collection.mutable.Map[String, (GCounter, Boolean)]] =
  readwriter[Map[String, (GCounter, Boolean)]].bimap(
    (m: scala.collection.mutable.Map[String, (GCounter, Boolean)]) => m.toMap,
    (m: Map[String, (GCounter, Boolean)]) => scala.collection.mutable.Map(m.toSeq: _*)
  )

/**
 * A case class for broadcasting the local window state.
 * The windowMap now uses String as the key.
 */
case class WindowState(
                        partition: Int,
                        vectorClock: Array[Long],
                        windowMap: scala.collection.mutable.Map[String, (GCounter, Boolean)]
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

  logger.info("Starting WindowedRecordProcFun")

  // The vector clock holds the highest event-time seen from each partition.
  private val vectorClock: Array[Long] = Array.fill(KAFKA_N_PARTITIONS)(0L)

  // Define a mutable map with the window (as a String) as the key.
  private val windowMap = scala.collection.mutable.Map.empty[String, (GCounter, Boolean)]

  override def process(
                        outputFunction: (Int, Byte, LogProducerRecords) => Unit,
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
              // TODO: Understand what dateTime represents in Nexmark context
              val eventTimestamp: Long = bid.dateTime

              // Update our own element in the vector clock.
              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

              // Determine the window for this event and convert to a String key.
              val window: Long = defineWindow(eventTimestamp)
              val windowKey: String = window.toString

              // Create or increment the window counter.
              if (!windowMap.contains(windowKey))
                windowMap(windowKey) = (GCounter.empty, false)
              else
                windowMap(windowKey) = (windowMap(windowKey)._1.increment(addr, 1L), false)
            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }
      case CHN_BROADCAST =>
        // Merge state received from other nodes.
        for (rec <- recs) {
          val receivedState = readBinary[WindowState](rec._2)
          val receivedVectorClock = receivedState.vectorClock
          val receivedWindowMap = receivedState.windowMap

          // Merge window maps element-wise.
          for ((k, v) <- receivedWindowMap) {
            if (windowMap.contains(k))
              windowMap(k) = (windowMap(k)._1.merge(v._1), windowMap(k)._2)
            else
              windowMap(k) = v
          }
          // Merge vector clocks element-wise.
          for (i <- 0 until KAFKA_N_PARTITIONS) {
            vectorClock(i) = math.max(vectorClock(i), receivedVectorClock(i))
          }
        }
      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // Get the current window and check if the previous window is ready to be emitted.
    val currentWin = defineWindow(vectorClock.min)

    if ((currentWin - 1) >= 0) {
      // Get latest window that can be closed
      val passedWindow: Long = currentWin - 1
      val passedWindowKey: String = passedWindow.toString
      if (windowMap.contains(passedWindowKey) && !windowMap(passedWindowKey)._2) {

        // Internal state update to indicate that the window is ready to be emitted.
        windowMap(passedWindowKey) = (windowMap(passedWindowKey)._1, true)

        // Output the final aggregate for the window. Logging for debug purposes.
        logger.info(s"partition: $partition, window: $passedWindow final aggregate: ${windowMap(passedWindowKey)._1.value}")
        outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(windowMap(passedWindowKey)._1.value))))
      }
    }

    // TODO: Reduce the number of broadcasts to free up network bandwidth.
    // Broadcast the current local state.
    val stateToBroadcast = WindowState(partition, vectorClock, windowMap)
    outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(stateToBroadcast))))
  }

  // Deterministically define the window for a given event time.
  override def defineWindow(eventTime: Long): Long = {
    val windowDuration: Long = 10_000L
    val time = eventTime / 10
    if (time % windowDuration == 0) time / windowDuration
    else (time / windowDuration) + 1
  }

  override def snapshot(): Array[Byte] = {
    // TODO: Implement full window state snapshot
    if (windowMap.nonEmpty) {
      // Store the crdt state for the latest window.
      crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, windowMap(windowMap.keys.max)._1)
    } else
      crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, GCounter.empty)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
      // TODO: To be implemented
      // val state = readBinary[WindowState](snapshot)
      // vectorClock.indices.foreach(i => vectorClock(i) = state.vectorClock(i))
      // windowMap.clear()
      // windowMap ++= state.windowMap
  }
}
