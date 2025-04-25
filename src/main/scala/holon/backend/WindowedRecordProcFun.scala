package holon.backend

import holon.*
import holon.crdt.CRDTWrapper
import holon.example.CRDT.address
import holon.example.{CRDT, Nexmark}
import holon.serialization.mutableMapReadWriter
import Config.*
import org.apache.pekko.cluster.ddata.SelfUniqueAddress
import upickle.legacy.{ReadWriter, macroRW, readBinary, writeBinary}

import scala.collection.mutable

// OutputState now includes the auction identifier with the most bids and the bid count.
case class OutputState(
                        partition: Int,
                        window: Long,
                        value: String
                      )
object OutputState {
  implicit val rw: ReadWriter[OutputState] = macroRW
}

// WindowState is generic in the CRDT type T.
case class WindowState[T](
                           partition: Int,
                           vectorClock: Array[Long],
                           windowMap: mutable.Map[Long, (T, Boolean)]
                         )
object WindowState {
  implicit def rw[T: ReadWriter]: ReadWriter[WindowState[T]] = macroRW
}

// This processing function maps bids to auctions and aggregates per window.
class WindowedRecordProcFun[T](partition: Int)(
  implicit crdt: CRDTWrapper[T],
  rw: ReadWriter[T]
) extends ProcFun {

  // Our unique address for this partition.
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")
  logger.info("Starting WindowedRecordProcFun")

  // Vector clock holding the highest event time seen from each partition.
  private val vectorClock: Array[Long] = Array.fill(nrOfKafkaPartitions())(0L)

  // The windowMap now holds a CRDT that tracks a map: auction id -> bid count.
  private val windowMap = mutable.Map.empty[Long, (T, Boolean)]
  // To keep track of windows that have already been emitted.
  private val emittedWindows = mutable.Set.empty[Long]

  // Garbage collection interval for old windows.
  private val gcInterval: Long = 500L

  override def process(
                        outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                        chn: Byte,
                        recs: LogConsumerRecords
                      ): Unit = {
    chn match {
      case CHN_INPUT =>
        // Process incoming events from the Nexmark stream.
        for (rec <- recs) {
          val event = Nexmark.deserialize(rec._2).event
          event match {
            case bid: Nexmark.Events.Bid =>
              // Get the event timestamp.
              val eventTimestamp: Long = bid.dateTime

              val window: Long = defineWindow(eventTimestamp)

              logger.debug(s"partition: $partition, event: $event, window: $window")
              if (!windowMap.contains(window)) {
                logger.debug(s"partition: $partition Creating new window: $window")
                windowMap(window) = (crdt.empty(addr), false)
              }
              // Increment the CRDT
              logger.debug(s"partition: $partition window: $window, incrementing crdt")
              val updated = crdt.increment(windowMap(window)._1, addr, bid)
              windowMap(window) = (updated, false)

              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }

      case CHN_BROADCAST =>
        logger.debug(s"partition: $partition, received broadcast from other partitions")
        // Merge state received from other nodes.
        for (rec <- recs) {
          val receivedState = readBinary[WindowState[T]](rec._2)
          val receivedPartition = receivedState.partition
          val receivedVectorClock = receivedState.vectorClock
          val receivedWindowMap = receivedState.windowMap
          logger.debug(s"partition: $partition Received broadcast from partition: $receivedPartition")

          // For each window in the received state, merge with our own state.
          for ((windowKey, receivedAggregate) <- receivedWindowMap) {
            if (windowMap.contains(windowKey) && !emittedWindows.contains(windowKey)) {
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

    // Determine the current window based on the vector clock.
    val currentLocalWin = defineWindow(vectorClock(partition))
    if ((currentLocalWin - 1) >= 0) {
      val passedWindow: Long = currentLocalWin - 1
      if (windowMap.contains(passedWindow) && !windowMap(passedWindow)._2) {
        // Mark the window as ready (closed) for emission.
        windowMap(passedWindow) = (windowMap(passedWindow)._1, true)

        logger.debug(s"Broadcasting window state for partition: $partition")
        // Only send the map of the passed window, including the key and the value.
        val windowState = WindowState(partition, vectorClock, windowMap.clone().filter(_._1 == passedWindow))
        logger.debug(s"partition: $partition, broadcasting window state: $windowState")
        outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(windowState))))

        logger.debug(s"partition: $partition, closed window: $passedWindow")
      }
    }

    // Garbage collect old windows at defined intervals.
    if (System.currentTimeMillis() % gcInterval < 10) {
      logger.debug(s"Garbage collecting windows for partition: $partition")
      garbageCollect()
    }

    // Check and emit windows which are closed and have converged.
    emitWindow(outputFunction)
  }

  private def emitWindow(outputFunction: (Int, Byte, LogProducerRecords) => Unit): Unit = {
    for ((windowKey, windowAggregate) <- windowMap) {
      // Emit the window if the CRDT state has been marked closed, hasn't yet been emitted,
      // and the oldest element in the vector clock is greater than the window key.
      if (windowAggregate._2 && !emittedWindows.contains(windowKey) && defineWindow(vectorClock.min) > windowKey) {
        val crdtValue: String = crdt.value(windowAggregate._1)
        if (crdtValue != null) {
          val outputState = OutputState(partition, windowKey, crdtValue)
          logger.debug(s"partition: $partition, emitting window: $windowKey, value: $outputState")
          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary(outputState))))
          emittedWindows += windowKey
        }
      }
    }
  }

  private def garbageCollect(): Unit = {
    if vectorClock.forall(_ == 0L) then return
    val currentLocalWin = defineWindow(vectorClock.min)
    if (currentLocalWin > 2) {
      var windowsToRemove: List[Long] = Nil
      windowsToRemove = emittedWindows.filter(_ < currentLocalWin - 2).toList
      if (windowsToRemove.nonEmpty) {
        logger.debug(s"partition: $partition, garbage collecting ${windowsToRemove.size} windows")
        for (windowKey <- windowsToRemove) {
          if (windowMap.contains(windowKey)) {
            windowMap -= windowKey
          }
        }
      }
    }
  }

  // Define the window for a given event time.
  override def defineWindow(eventTime: Long): Long = {
    val time = eventTime / 10
    val WL = 10_000L
    if (time % WL == 0) time / WL else (time / WL) + 1
  }

  override def snapshot(): Array[Byte] = {
    logger.debug("Taking snapshot")
    writeBinary(windowMap.filter(!_._2._2).toMap)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    logger.debug("Restoring from snapshot")
    if (snapshot == null) {
      logger.debug("Snapshot is null, skipping restore")
      return
    }
    val restoredMap = readBinary[mutable.Map[Long, (T, Boolean)]](snapshot)
    windowMap.clear()
    windowMap ++= restoredMap.toMap
  }
}