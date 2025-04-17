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
// More succinct ReadWriter:
given ReadWriter[OutputState] = macroRW
// object OutputState {
//   implicit val rw: ReadWriter[OutputState] = macroRW
// }

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
  // Perhaps better if not implicit...
  crdt: CRDTWrapper[T],
  rw: ReadWriter[T]
) extends ProcFun {

  // Our unique address for this partition.
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")
  logger.info("Starting WindowedRecordProcFun")

  // Vector clock holding the highest event time seen from each partition.
  // Perhaps better to say this: The vector clock holds the low watermark of events processed at each partition.
  private val vectorClock: Array[Long] = Array.fill(nrOfKafkaPartitions())(0L)

  // Outdated comment.
  // The windowMap now holds a CRDT that tracks a map: auction id -> bid count.
  private val windowMap = mutable.Map.empty[Long, (T, Boolean)]

  // It would probably suffice only keep a counter of the maximum window, as I'd expect that windows are emitted in order.
  // To keep track of windows that have already been emitted.
  private val emittedWindows = mutable.Set.empty[Long]

  // Garbage collection interval for old windows.
  // Could be a parameter?
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

            // Perhaps it would make sense to make the WCRDT Proc Fun general so that:
            // 1. We can define it for a general type of CRDT
            // 2. We can define it for a general type of event
            // It seems as though (1) is done, but (2) is not. Doing (2) would
            // make it easier to write queries on new types of events, reducing
            // duplicated code.
            case bid: Nexmark.Events.Bid =>
              // Get the event timestamp.
              val eventTimestamp: Long = bid.dateTime

              val window: Long = defineWindow(eventTimestamp)

              if (!windowMap.contains(window)) {
                windowMap(window) = (crdt.empty, false)
              }
              // Comments seem outdated.
              // Increment the bid count for this auction.
              val updated = crdt.increment(windowMap(window)._1, addr, bid)
              windowMap(window) = (updated, false)

              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }

      // Perhaps there is a better solution than using channels... it seems a bit flaky.
      case CHN_BROADCAST =>
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

    // Is it not possible that a window is not emitted here?
    // What if currentLocalWin is 19, but 17 was not emitted. I don't immediately
    // see how this case is not an issue.
    if (currentLocalWin >= 1) {
      val passedWindow: Long = currentLocalWin - 1
      if (windowMap.contains(passedWindow) && !windowMap(passedWindow)._2) {
        // Mark the window as ready (closed) for emission.
        windowMap(passedWindow) = (windowMap(passedWindow)._1, true)

        logger.debug(s"Broadcasting window state for partition: $partition")
        // Only send the map of the passed window, including the key and the value.
        val windowState = WindowState(partition, vectorClock, windowMap.clone().filter(_._1 == passedWindow))
        outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(windowState))))

        logger.debug(s"partition: $partition, closed window: $passedWindow")
      }
    }

    // Garbage collect old windows at defined intervals.
    // It seems as though it is possible for GC to never occur if we avoid sampling just within 10 ms from the interval.
    // Probably a better idea to save the latest GC timestamp, and then perform GC
    // the next time that timestmap is passed.
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
      // I would think that it is unnecessary to check windowAggregate._2 here.
      if (windowAggregate._2 && !emittedWindows.contains(windowKey) && defineWindow(vectorClock.min) > windowKey) {
        // Why is the CRDT value a string?
        val crdtValue: String = crdt.value(windowAggregate._1)
        if (crdtValue != null) {
          // It may make sense to output an event even when the window is empty.
          // Otherwise, how does the consumer know if the window was empty?
          val outputState = OutputState(partition, windowKey, crdtValue)
          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary(outputState))))
          emittedWindows += windowKey
        }
      }
    }
  }

  private def garbageCollect(): Unit = {
    if vectorClock.forall(_ == 0L) then return
    val currentLocalWin = defineWindow(vectorClock.min)
    // whe minus two? and > 2?
    if (currentLocalWin > 2) {
      var windowsToRemove: List[Long] = Nil
      windowsToRemove = emittedWindows.filter(_ < currentLocalWin - 2).toList
      if (windowsToRemove.nonEmpty) {
        logger.info(s"partition: $partition, garbage collecting ${windowsToRemove.size} windows")
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
    // Window Duration should be a parameter to the class.
    val windowDuration: Long = 10_000L
    val time = eventTime / 10
    if (time % windowDuration == 0) time / windowDuration else (time / windowDuration) + 1
  }

  // The snapshot currently doesn't store the vector clock or the emitted windows...
  // Should it store them? It seems reasonable to store them.
  override def snapshot(): Array[Byte] = {
    logger.debug("Taking snapshot")
    // It would seem reasonable to also store windows that are not 'closed' here.
    // Is it not the case that a snapshot may be taken when windows are not yet closed.
    // After recovery, the processing will continue from the latest committed index from the input stream,
    // and the latest committed snapshot. So, potentially, events that were in a partially closed window
    // are now not replayed and lost. Check with Kolya what is the case.
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
