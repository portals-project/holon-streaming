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

case class OutputState(
                        partition: Int,
                        window: Long,
                        value: String
                      )

object OutputState {
  given ReadWriter[OutputState] = macroRW
}

case class WindowState[T](
   partition: Int,
   queryId: Int,
   vectorClock: Array[Long],
   windowMap: mutable.Map[Long, (T, Boolean)]
 )
object WindowState {
  given windowStateRW[T: ReadWriter]: ReadWriter[WindowState[T]] = macroRW
}

case class SnapShotState[T](
   emittedWindows: Long,
   vectorClock: Array[Long],
   windowMap: mutable.Map[Long, (T, Boolean)]
 )
object SnapShotState {
  given snapShotStateRW[T: ReadWriter]: ReadWriter[SnapShotState[T]] = macroRW
}

// This processing function maps bids to auctions and aggregates per window.
case class WindowedRecordProcFun[T, V](crdt: CRDTWrapper[T, V], partition: Int, queryId: Int = 0, rw: ReadWriter[T]) extends ProcFun {
  private val logger = Logger("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")
  logger.info("Starting WindowedRecordProcFun")

  // Custom serialization for the CRDT type T.
  implicit val elementRW: ReadWriter[T] = rw

  // Our unique address for this partition.
  private val addr: SelfUniqueAddress = address(partition)

  // The vector clock holds the low watermark of events processed at each partition.
  var vectorClock: Array[Long] = Array.fill(nrOfKafkaPartitions())(0L)

  // The windowMap holds the state of each local window.
  val windowMap = mutable.Map.empty[Long, (T, Boolean)]

  // To keep track of windows that have already been emitted.
  private var emittedWindows = 0L

  // TODO delete: Only used for benchmarking.
  // Keep track of Kafka log append times of the last record for this window.
  private val logAppendTimePerWindow = mutable.Map.empty[Long, Long]

  override def processInput(
                        outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                        chn: Byte,
                        recs: LogConsumerRecords
                      ): Unit = {
    chn match {
      case CHN_INPUT =>
        // Process incoming events from the Nexmark stream.
        for (rec <- recs) {
          val tsEvent = Nexmark.deserialize(rec._2)
          crdt.checkType(tsEvent) match {
            case Some(event) =>
              // Get the event timestamp.
              val eventTimestamp: Long = crdt.timeStamp(event)

              // Get current window
              val window: Long = defineWindow(eventTimestamp)

              // Update windowMap
              if (!windowMap.contains(window)) {
                logger.debug(s"partition: $partition Creating new window: $window")
                windowMap(window) = (crdt.empty(addr), false)
              }

              // Increment the CRDT and vector clock.
              logger.debug(s"partition: $partition window: $window, incrementing crdt")
              val updated = crdt.update(windowMap(window)._1, addr, event)
              windowMap(window) = (updated, false)

              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

              // Update the log append time for this window.
              logAppendTimePerWindow(window) =
                math.max(logAppendTimePerWindow.getOrElse(window, 0L), rec._3)

            case None =>
              logger.debug(s"Ignored non-bid event")
          }
        }
      // TODO: Find alternative for using channels.
      case CHN_BROADCAST =>
        logger.debug(s"partition: $partition, received broadcast from other partitions")
        // Merge state received from other nodes.
        for (rec <- recs) {
          val receivedState = readBinary[WindowState[T]](rec._2)
          // Process the received state if it matches our queryId (for multiple queries).
          // TODO: Find and add alternative for using queryId.
          if (receivedState.queryId == queryId) {
            val receivedPartition = receivedState.partition
            logger.debug(s"partition: $partition Received broadcast from partition: $receivedPartition")
            // For each window in the received state, merge with our own state.
            for ((windowKey, receivedAggregate) <- receivedState.windowMap) {
              if (windowMap.contains(windowKey)) {
                val merged = crdt.merge(windowMap(windowKey)._1, receivedAggregate._1)
                windowMap(windowKey) = (merged, windowMap(windowKey)._2)
              } else {
                windowMap(windowKey) = receivedAggregate
              }
            }
            // Merge vector clock for the received partition.
            vectorClock(receivedPartition) =
              math.max(vectorClock(receivedPartition), receivedState.vectorClock(receivedPartition))
          }
        }
      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // Determine the current window based on the vector clock.
    val passedWindow: Long = defineWindow(vectorClock(partition)) - 1
    if (passedWindow > 0 && windowMap.contains(passedWindow) && !windowMap(passedWindow)._2) {
      // Mark the window as ready (closed) for emission.
      windowMap(passedWindow) = (windowMap(passedWindow)._1, true)
      // Only send the map of the passed window, including the key and the value.
      val windowState = WindowState(partition,  queryId, vectorClock, windowMap.clone().filter(_._1 == passedWindow))
      logger.debug(s"partition: $partition, broadcasting window state: $windowState")
      outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(windowState))))

      emittedWindows = math.max(emittedWindows, passedWindow)
    }
  }

  // Define the window for a given event time.
  def defineWindow(eventTime: Long): Long = {
    val time = eventTime / 10
    if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH else (time / WINDOW_LENGTH) + 1
  }

  def garbageCollect(windowKey: Long): Unit = {
    logger.debug(s"partition: $partition, garbage collecting for window key: ${windowKey}")
    if (windowMap.contains(windowKey)) {
      windowMap -= windowKey
    }
    if (logAppendTimePerWindow.contains(windowKey)) {
      logAppendTimePerWindow -= windowKey
    }
  }

  override def snapshot(): Array[Byte] = {
    logger.debug("Taking snapshot")
    val snapshotState = SnapShotState(emittedWindows, vectorClock, windowMap.clone())
    writeBinary[SnapShotState[T]](snapshotState)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    logger.debug("Restoring from snapshot")
    if (snapshot == null) {
      logger.debug("Snapshot is null, skipping restore")
      return
    }
    val restoredMap = readBinary[SnapShotState[T]](snapshot)
    windowMap.clear()
    windowMap ++= restoredMap.windowMap
    vectorClock = restoredMap.vectorClock
    emittedWindows = restoredMap.emittedWindows
  }
}