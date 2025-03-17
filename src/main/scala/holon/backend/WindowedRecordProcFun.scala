package holon.backend

import holon.*
import holon.example.CRDT.*
import holon.example.nexmark.Config.*
import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}
import upickle.default.{ReadWriter, macroRW, readBinary, readwriter, writeBinary}

import scala.util.Random

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

  logger.info("Starting WindowedRecordProcFun")

  // The vector clock holds the highest event-time seen from each partition.
  private val vectorClock: Array[Long] = Array.fill(KAFKA_N_PARTITIONS)(0L)

  // Define a mutable map with the window as the key.
  private val windowMap = scala.collection.mutable.Map.empty[Long, (GCounter, Boolean)]

  // Map to make partition functions idempotent
  private val partitionMap = scala.collection.mutable.Map.empty[Long, scala.collection.mutable.Map[Int, (Boolean, BigInt)]]

  // Map to keep track of all windows that have been emitted.
  private val emittedWindows = scala.collection.mutable.Set.empty[Long]

  // Message count for broadcast frequency.
  private var messageCount = 0

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

              // Get the previous window if window is not 0.
              val prevWindow: Long = if (window > 0) window - 1 else 0

              // Create or increment the window counter.
              if (!windowMap.contains(window))
                windowMap(window) = (GCounter.empty, false)
                for i <- 0 until KAFKA_N_PARTITIONS do
                  if !partitionMap.contains(window) then
                    partitionMap(window) = scala.collection.mutable.Map.empty[Int, (Boolean, BigInt)]
                  if !partitionMap(window).contains(i) then
                    partitionMap(window)(i) = (false, BigInt(0))
              else
                windowMap(window) = (windowMap(window)._1.increment(addr, 1L), false)
            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }
      case CHN_BROADCAST =>
        // Merge state received from other nodes.
        for (rec <- recs) {
          val receivedState = readBinary[WindowState](rec._2)
          val receivedPartition = receivedState.partition
          val receivedVectorClock = receivedState.vectorClock
          val receivedWindowMap = receivedState.windowMap

          // Merge the window map depending on the type of message received.
          if (receivedPartition != -1) {
            for ((k, v) <- receivedWindowMap) {
              if (windowMap.contains(k)){
                windowMap(k) = (windowMap(k)._1.merge(v._1), windowMap(k)._2)
                if (v._2) {
                  if (!partitionMap.contains(k))
                    partitionMap(k) = scala.collection.mutable.Map(receivedPartition -> (v._2, windowMap(k)._1.value))
                  else
                    partitionMap(k)(receivedPartition) = (v._2, v._1.value)
//                  logger.info(s"partition: $partition, window: $k partition map: ${partitionMap}")
                }
              //                    outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(windowMap(k)._1.value))))
              }
              else
                windowMap(k) = v
                partitionMap(k) = scala.collection.mutable.Map(receivedPartition -> (v._2, v._1.value))
            }
          }
          // Merge vector clocks element-wise.
          for (i <- 0 until KAFKA_N_PARTITIONS) {
            vectorClock(i) = math.max(vectorClock(i), receivedVectorClock(i))
          }

          // Output the final aggregate for the window. Logging for debug purposes.
        }
      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // Get the current window and check if the previous window is ready to be emitted.
    val currentWin = defineWindow(vectorClock.min)

    if ((currentWin - 1) >= 0) {
      // Get latest window that can be closed
      val passedWindow: Long = currentWin - 1
      if (windowMap.contains(passedWindow) && !windowMap(passedWindow)._2) {
        // TODO: Determine if we delete closed windows from the windowMap.
        // Internal state update to indicate that the window is ready to be emitted.
        windowMap(passedWindow) = (windowMap(passedWindow)._1, true)

//        logger.info(s"partition: $partition, window map: $windowMap")

        // Output the final aggregate for the window. Logging for debug purposes.
//        logger.info(s"partition: $partition, window: $passedWindow final aggregate: ${windowMap(passedWindow)._1.value}")
        //        outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(windowMap(passedWindow)._1.value))))
      }
    }

    // TODO: Find a better way to time the emission of the final aggregate to make sure crdts have converged.
    for ((k, v) <- windowMap) {
      // Only emit the final aggregate when all these conditions are met.
      // 1. Each partition has closed the window.
      // 2. The window has not been emitted before.
      // 3. All the partitions have the same aggregate value.
      if (v._2 && !emittedWindows.contains(k) && partitionMap.contains(k)) {
        val values = partitionMap(k).values.map(_._2).toList
        if (values.distinct.size == 1) {
//            logger.info(s"partition: $partition, window: $k final aggregate: ${windowMap(k)._1.value}")
          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary(windowMap(k)._1.value))))
          emittedWindows += k
        }
      }
    }

    // TODO: Find a better way to reduce message count.
    messageCount += 1
    if messageCount >= 30 then
      // Broadcast the current local state.
      val stateToBroadcast = WindowState(partition, vectorClock, windowMap)
      outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(stateToBroadcast))))
      messageCount = 0

    // Broadcast only the vector clock more frequently.
    if messageCount % 10 == 0 then
      val vectorClockState = WindowState(-1, vectorClock, scala.collection.mutable.Map.empty[Long, (GCounter, Boolean)])
      outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(vectorClockState))))
  }

  // Deterministically define the window for a given event time.
  override def defineWindow(eventTime: Long): Long = {
    val windowDuration: Long = 10_000L
    val time = eventTime / 10
    if (time % windowDuration == 0) time / windowDuration
    else (time / windowDuration) + 1
  }

  override def snapshot(): Array[Byte] = {
    logger.info("Taking snapshot")
    // TODO: Implement full window state snapshot
    if (windowMap.nonEmpty) {
      // Create a sequence of all windows and gcounters that have not been closed.
      writeBinary(windowMap.filter(!_._2._2).toMap)
    } else
      writeBinary(scala.collection.mutable.Map.empty[Long, (GCounter, Boolean)])
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    logger.info("Restoring from snapshot")
    val restoredMap = readBinary[scala.collection.mutable.Map[Long, (GCounter, Boolean)]](snapshot)
    logger.info(s"Restored window map: $restoredMap")
    windowMap.clear()
    windowMap ++= restoredMap.toMap
  }
}
