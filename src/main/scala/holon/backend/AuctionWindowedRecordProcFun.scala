package holon.backend

import holon._
import holon.crdt.CRDTWrapper
import holon.example.CRDT.address
import holon.example.nexmark.Config._
import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}
import upickle.default._
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
                        auctionId: String,
                        bidCount: BigInt
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
class AuctionWindowedRecordProcFun[T](partition: Int)(
  implicit crdt: CRDTWrapper[T],
  rw: ReadWriter[T]
) extends ProcFun {

  // Our unique address for this partition.
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger("AuctionWindowedRecordProcFun")
  Logger.setLevel("AuctionWindowedRecordProcFun", "INFO")
  logger.info("Starting AuctionWindowedRecordProcFun")

  // Vector clock holding the highest event time seen from each partition.
  private val vectorClock: Array[Long] = Array.fill(KAFKA_N_PARTITIONS)(0L)

  // The windowMap now holds a CRDT that tracks a map: auction id -> bid count.
  private val windowMap = mutable.Map.empty[Long, (T, Boolean)]
  // To keep track of windows that have already been emitted.
  private val emittedWindows = mutable.Set.empty[Long]

  // Broadcast and garbage collection intervals.
  private val broadcastInterval: Long = 200L
  private val gcInterval: Long = 1000L

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
              // Get the event timestamp.
              val eventTimestamp: Long = bid.dateTime
              // Determine the window for the event.
              val window: Long = defineWindow(eventTimestamp)
              // Extract the auction ID from the bid (ensure bid.auctionId is available).
              val auctionId: Long = bid.auction
              // Create a new window if one does not exist.
              if (!windowMap.contains(window)) {
                windowMap(window) = (crdt.empty, false)
              }
              // Increment the bid count for this auction.
              val updated = crdt.increment(windowMap(window)._1, addr, 1L, auctionId.toString)
              windowMap(window) = (updated, false)
              // Update our vector clock.
              vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

            case other =>
              logger.debug(s"Ignored non-bid event: $other")
          }
        }

      case CHN_BROADCAST =>
        // Merge state received from other nodes.
        for (rec <- recs) {
          val receivedState = readBinary[WindowState[T]](rec._2)
          val receivedPartition = receivedState.partition
          val receivedVectorClock = receivedState.vectorClock
          val receivedWindowMap = receivedState.windowMap

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
        logger.debug(s"partition: $partition, closed window: $passedWindow")
      }
    }

    // Broadcast state at defined intervals.
    if (System.currentTimeMillis() % broadcastInterval < 10) {
      logger.debug(s"Broadcasting window state for partition: $partition")
      val windowState = WindowState(partition, vectorClock.clone(), windowMap.clone())
      outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(windowState))))
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
      // and the oldest element in the vector clock is greater than the window key (ensuring convergence).
      if (windowAggregate._2 && !emittedWindows.contains(windowKey) && defineWindow(vectorClock.min) > windowKey) {
        // Convert the CRDT state to a Map[String, BigInt] (auction id -> bid count).
        // It is assumed that crdt.value returns a Map[String, BigInt] when using the map-based CRDT.
        val auctionBidMap: Map[String, BigInt] = crdt.value(windowAggregate._1, "crdt-map")
        if (auctionBidMap.nonEmpty) {
          // Select the auction with the maximum bid count.
          val (maxAuctionId, maxCount) = auctionBidMap.maxBy(_._2)
          val outputState = OutputState(partition, windowKey, maxAuctionId, maxCount)
          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(outputState))))
          emittedWindows += windowKey
        }
      }
    }
  }

  private def garbageCollect(): Unit = {
    if vectorClock.forall(_ == 0L) then return
    val currentLocalWin = defineWindow(vectorClock.min)
    if (currentLocalWin > 2) {
      val windowsToRemove = emittedWindows.filter(_ < currentLocalWin - 5).toList
      if (windowsToRemove.nonEmpty) {
        for (windowKey <- windowsToRemove) {
          if (windowMap.contains(windowKey)) {
            logger.info(s"partition: $partition, garbage collecting window: $windowKey")
            windowMap -= windowKey
          }
        }
      }
    }
  }

  // Define the window for a given event time.
  override def defineWindow(eventTime: Long): Long = {
    val windowDuration: Long = 10_000L
    val time = eventTime / 10
    if (time % windowDuration == 0) time / windowDuration else (time / windowDuration) + 1
  }

  override def snapshot(): Array[Byte] = {
    logger.info("Taking snapshot")
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
