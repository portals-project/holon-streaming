package holon.backend

import holon.*
import holon.crdt.CRDTWrapper
import holon.example.CRDT.address
import holon.example.{CRDT, Nexmark}
import Config.*
import org.apache.pekko.cluster.ddata.SelfUniqueAddress
import upickle.legacy.{ReadWriter, macroRW, readBinary, readwriter, writeBinary}

import scala.collection.mutable

// Generic implicit for mutable maps
implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[scala.collection.mutable.Map[K, V]] =
  readwriter[Map[K, V]].bimap[scala.collection.mutable.Map[K, V]](
    _.toMap,
    m => scala.collection.mutable.Map.empty[K, V] ++ m
  )

// OutputState now includes the auction identifier with the most bids and the bid count.
case class OutputState(
                        partition: Int,
                        window: Long,
                        value: String
                      )
object OutputState {
  implicit def rw: ReadWriter[OutputState] = macroRW
}

// WindowState is generic in the CRDT type T.
case class WindowState[T](
                           partition: Int,
                           queryId: Int,
                           vectorClock: Array[Long],
                           windowMap: mutable.Map[Long, (T, Boolean)]
                         )
object WindowState {
  implicit def rw[T: ReadWriter]: ReadWriter[WindowState[T]] = macroRW
}

// This processing function maps bids to auctions and aggregates per window.
case class WindowedRecordProcFun[T, V](crdt: CRDTWrapper[T, V], partition: Int, queryId: Int = 0, rw: ReadWriter[T], rwout: ReadWriter[V]) extends ProcFun {

  // Our unique address for this partition.
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")
  logger.info("Starting WindowedRecordProcFun")

  // Vector clock holding the highest event time seen from each partition.
  val vectorClock: Array[Long] = Array.fill(nrOfKafkaPartitions())(0L)

  // The windowMap now holds a CRDT that tracks a map: auction id -> bid count.
  val windowMap = mutable.Map.empty[Long, (T, Boolean)]
  // To keep track of windows that have already been emitted.
  private val emittedWindows = mutable.Set.empty[Long]

  // TODO delete: Only used for benchmarking.
  // Keep track of Kafka log append times of the last record for this window.
  private val logAppendTimePerWindow = mutable.Map.empty[Long, Long]

  // Garbage collection interval for old windows.
  private val gcInterval: Long = 500L

  implicit val elementRW: ReadWriter[T] = rw
  
  implicit val outputStateRW: ReadWriter[V] = rwout

  override def processInput(
                        outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                        chn: Byte,
                        recs: LogConsumerRecords
                      ): Unit = {
    chn match {
      case CHN_INPUT =>
        // Process incoming events from the Nexmark stream.
        for (rec <- recs) {
//          val event = Nexmark.deserialize(rec._2).event
          val tsEvent = Nexmark.deserialize(rec._2)
          crdt.checkType(tsEvent) match {
            case Some(event) =>
              // Get the event timestamp.
              val eventTimestamp: Long = crdt.timeStamp(event)

              val window: Long = defineWindow(eventTimestamp)

              logger.debug(s"partition: $partition, event: $event, window: $window")
              if (!windowMap.contains(window)) {
                logger.debug(s"partition: $partition Creating new window: $window")
                windowMap(window) = (crdt.empty(addr), false)
              }
              // Increment the CRDT
              logger.debug(s"partition: $partition window: $window, incrementing crdt")
              val updated = crdt.update(windowMap(window)._1, addr, event)
              windowMap(window) = (updated, false)

              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

              logAppendTimePerWindow(window) =
                math.max(logAppendTimePerWindow.getOrElse(window, 0L), rec._3)

            case None =>
              logger.debug(s"Ignored non-bid event")
          }
        }
      case CHN_BROADCAST =>
        logger.debug(s"partition: $partition, received broadcast from other partitions")
        // Merge state received from other nodes.
        for (rec <- recs) {
          val receivedState = readBinary[WindowState[T]](rec._2)
          if (receivedState.queryId == queryId) {
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
        val windowState = WindowState(partition,  queryId, vectorClock, windowMap.clone().filter(_._1 == passedWindow))
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
//    emitWindow(outputFunction)
  }

  private def emitWindow(outputFunction: (Int, Byte, LogProducerRecords) => Unit): Unit = {
    for ((windowKey, windowAggregate) <- windowMap) {
      // Emit the window if the CRDT state has been marked closed, hasn't yet been emitted,
      // and the oldest element in the vector clock is greater than the window key.
      if (windowAggregate._2 && !emittedWindows.contains(windowKey) && defineWindow(vectorClock.min) > windowKey) {
        val crdtValue = crdt.value(windowAggregate._1).toString
        if (crdtValue != null) {
          val outputState = OutputState(partition, windowKey, crdtValue)
          logger.debug(s"partition: $partition, emitting window: $windowKey, value: $outputState")
          logger.info(s"[LagAppendInput] - window: $windowKey, timestamp: ${logAppendTimePerWindow.getOrElse(windowKey, 0L)}")
          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](outputState))))
          emittedWindows += windowKey
        }
      }
    }
  }

  // Define the window for a given event time.
  def defineWindow(eventTime: Long): Long = {
    val time = eventTime / 10
    if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH else (time / WINDOW_LENGTH) + 1
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
          if (logAppendTimePerWindow.contains(windowKey)) {
            logAppendTimePerWindow -= windowKey
          }
        }
      }
    }
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