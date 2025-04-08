package holon.backend

import holon.*
import holon.crdt.CRDTWrapper
import holon.example.CRDT.address
import holon.example.nexmark.Config.*
import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}
import upickle.default.*

import scala.collection.mutable

// A generic implicit for mutable maps (if needed for other types)
implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[scala.collection.mutable.Map[K, V]] =
  readwriter[Map[K, V]].bimap[scala.collection.mutable.Map[K, V]](
    _.toMap,
    m => scala.collection.mutable.Map.empty[K, V] ++ m
  )

// A case class for broadcasting the local window state, now generic in CRDT type T.
case class WindowState[T](
                           partition: Int,
                           vectorClock: Array[Long],
                           windowMap: mutable.Map[Long, (T, Boolean)]
                         )
object WindowState {
  implicit def rw[T: ReadWriter]: ReadWriter[WindowState[T]] = macroRW
}

// A case class for the output value of the final aggregation.
case class OutputState(
                        partition: Int,
                        window: Long,
                        value: BigInt
                      )
object OutputState {
  implicit val rw: ReadWriter[OutputState] = macroRW
}

class WindowedRecordProcFun[T](partition: Int)(
  implicit
    crdt: CRDTWrapper[T],
    rw: ReadWriter[T])
extends ProcFun {
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")
  logger.info("Starting WindowedRecordProcFun")

  // The vector clock holds the highest event-time seen from each partition.
  private val vectorClock: Array[Long] = Array.fill(KAFKA_N_PARTITIONS)(0L)

  // Define a mutable map with the window as the key.
  private val windowMap = mutable.Map.empty[Long, (T, Boolean)]

  // Map to keep track of all windows that have been emitted.
  private val emittedWindows = mutable.Set.empty[Long]

  // Broadcast every 500 milliseconds
  private val broadcastInterval: Long = 200L

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

              // Determine the window for this event.
              val window: Long = defineWindow(eventTimestamp)

              // Create or increment the window and partition map.
              if (!windowMap.contains(window)) {
                windowMap(window) = (crdt.empty, false)
              } else {
                val updated = crdt.increment(windowMap(window)._1, addr, 1L)
                windowMap(window) = (updated, false)
              }

              // Update our own element in the vector clock.
              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }

      case CHN_BROADCAST =>
        // Merge state received from other nodes.
        for (rec <- recs) {
          val receivedState = readBinary[WindowState[T]](rec._2)
          // Deconstruct the received state.
          val receivedPartition = receivedState.partition
          val receivedVectorClock = receivedState.vectorClock
          val receivedWindowMap = receivedState.windowMap

          // Merge the window map depending on the type of message received.
          for ((windowKey, receivedAggregate) <- receivedWindowMap) {
            if (windowMap.contains(windowKey)) {
              val merged = crdt.merge(windowMap(windowKey)._1, receivedAggregate._1)
              windowMap(windowKey) = (merged, windowMap(windowKey)._2)
            } else {
              windowMap(windowKey) = receivedAggregate
            }
          }

          // Merge vector clocks element-wise.
          vectorClock(receivedPartition) =
            math.max(vectorClock(receivedPartition), receivedVectorClock(receivedPartition))
        }

      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // Get the current window and check if the previous window is ready to be emitted.
    val currentLocalWin = defineWindow(vectorClock(partition))

    // Check if the previous window is ready to be closed.
    if ((currentLocalWin - 1) >= 0) {
      // Get latest window that can be closed
      val passedWindow: Long = currentLocalWin - 1
      if (windowMap.contains(passedWindow) && !windowMap(passedWindow)._2) {
        // Internal state update to indicate that the window is ready to be emitted.
        windowMap(passedWindow) = (windowMap(passedWindow)._1, true)
        logger.debug(s"partition: $partition, closed window: $passedWindow")
      }
    }

    // Broadcast every broadcastInterval milliseconds
    if (System.currentTimeMillis() % broadcastInterval < 10) {
      logger.debug(s"Broadcasting window state for partition: $partition")
      val windowState = WindowState(partition, vectorClock.clone(), windowMap.clone())
      outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(windowState))))
    }

    // See if we can emit windows
    emitWindow(outputFunction)
  }

  private def emitWindow(outputFunction: (Int, Byte, LogProducerRecords) => Unit): Unit = {
    // Emit the final aggregate
    // TODO: Find a better way to time the emission of the final aggregate to make sure crdts have converged.
    for ((windowKey, windowAggregate) <- windowMap) {
      // Only emit the final aggregate when all these conditions are met.
      // 1. Each partition has closed the window, I.e The vector clock is greater than the window key.
      // 2. The window has not been emitted before.
      if (windowAggregate._2 && !emittedWindows.contains(windowKey) && defineWindow(vectorClock.min) > windowKey) {
//          logger.info(s"partition: $partition, window: $windowKey final aggregate: ${crdt.value(windowAggregate._1)}")
          val outputState = OutputState(partition, windowKey, crdt.value(windowAggregate._1))
          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(outputState))))
          emittedWindows += windowKey
      }
    }
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
    // Create a sequence of all windows and CRDTs that have not been closed.
    writeBinary(windowMap.filter(!_._2._2).toMap)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    logger.info("Restoring from snapshot")
    val restoredMap = readBinary[mutable.Map[Long, (T, Boolean)]](snapshot)
    logger.info(s"Restored window map: $restoredMap")

    windowMap.clear()
    windowMap ++= restoredMap.toMap
  }
}
