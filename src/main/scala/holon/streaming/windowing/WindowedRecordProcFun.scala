package holon.streaming.windowing

import holon.utils.*
import holon.crdt.CRDTWrapper
import holon.crdt.CRDT
import holon.examples.nexmark.data.Nexmark
import holon.crdt.CRDT.address
import holon.core.Config.*
import holon.streaming.processing.ProcFun
import org.apache.pekko.cluster.ddata.{ReplicatedDelta, SelfUniqueAddress}
import org.slf4j.LoggerFactory
import upickle.legacy.{ReadWriter, readBinary, writeBinary}

import scala.collection.mutable

// Import general serializations
import holon.crdt.serialization.{DeltaCRDTSerialization, WindowStateSerialization}
import DeltaCRDTSerialization.deltaRW
import WindowStateSerialization.{WindowDelta, WindowState, SnapShotState, OutputState}

// this processing function maps bids to auctions and aggregates per window.
case class WindowedRecordProcFun[T, V](crdt: CRDTWrapper[T, V], partition: Int, queryId: Int = 0, rw:ReadWriter[T]) extends ProcFun {
  private val logger = Logger("holon.streaming.windowing.WindowedRecordProcFun")
  private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("holon.streaming.windowing.WindowedRecordProcFun", "INFO")
  logger.info(s"Starting holon.streaming.windowing.WindowedRecordProcFun for partition: $partition")

  // whenever you need an implicit ReadWriter[T], unwrap or throw
  implicit val elementRW: ReadWriter[T] = rw
  private val addr: SelfUniqueAddress = address(partition)

  var vectorClock: Array[Long] = Array.fill(nrOfKafkaPartitions())(0L)
  val windowMap = mutable.Map.empty[Long, (T, Boolean)]
  private var emittedWindows = 0L
  var queriedWindow: Long = 1L

  val lagAppendTimePerWindow = mutable.Map.empty[Long, Long]

  // delta support
  private val deltaMap = mutable.Map.empty[Long, List[ReplicatedDelta]]
  private val completionBits = mutable.Map.empty[Long, java.util.BitSet]
  private val flushThreshold = FLUSH_THRESHOLD
  private var eventsSinceLastFlush = 0
  // for each window w, a map from partition → its final watermark ts
  private val pendingFinal = mutable.Map.empty[Long, mutable.Map[Int, Long]]

  override def processInput(
                             outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                             chn: Byte,
                             recs: LogConsumerRecords
                           ): Unit = {
    val rec = recs.head
    chn match {
      case CHN_INPUT =>
        val tsEvent = holon.examples.nexmark.data.Nexmark.deserialize(rec._2)
        crdt.checkType(tsEvent) match {
          case Some(event) =>
            val eventTimestamp = crdt.timeStamp(event)
            val window = defineWindow(eventTimestamp)

            if (!windowMap.contains(window)) {
              logger.debug(s"partition: $partition Creating new window: $window")
              windowMap(window) = (crdt.empty(addr), false)
            }

            // log state size every 5 seconds
           if (System.currentTimeMillis() % 5000 < 100) {
              // wherever a window is closed, emit state size
              val serializedState: Array[Byte] = writeBinary(windowMap)
              if USE_LOG_FILE then
                outputLog.info(s"[STATE-SIZE]: partition: $partition, size: ${serializedState.length}, timestamp: ${System.currentTimeMillis()}, queryId: $queryId")
              else
                logger.info(s"[STATE-SIZE]: partition: $partition, size: ${serializedState.length}, timestamp: ${System.currentTimeMillis()}, queryId: $queryId")
            }

            // delta update
            val (updated, maybeDelta) = crdt.updateWithDelta(windowMap(window)._1, addr, event)
            windowMap(window) = (updated, windowMap(window)._2)

            // only if a delta actually exists do we store/broadcast it
            maybeDelta.foreach { delta =>
              deltaMap.update(window, delta :: deltaMap.getOrElse(window, Nil))
            }
            eventsSinceLastFlush += 1

            // periodic flush of intermediate deltas
            if (eventsSinceLastFlush >= flushThreshold) {
              flushDeltas(outputFunction)
              eventsSinceLastFlush = 0
            }

            // advance vector clock
            vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

            // try to mark final for all windows
            pendingFinal.keys.foreach(tryMarkFinal)

            // local window‐close detection & final‐delta
            val locallyClosed = defineWindow(vectorClock(partition)) - 1L
            if (locallyClosed > queriedWindow) {
              // for each window that is closed, mark it as closed and send the final delta
              for (w <- queriedWindow until locallyClosed if !windowMap.get(w).exists(_._2)) {
                val (st, _) = windowMap.getOrElse(w, (crdt.empty(addr), false))
                // mark closed & send final‐delta
                windowMap(w) = (st, true)
                flushFinalDelta(w, outputFunction)
              }
              queriedWindow = locallyClosed
            }

            // optional benchmark logging
            if (queryId == 0) {
              lagAppendTimePerWindow(window) = math.max(lagAppendTimePerWindow.getOrElse(window, 0L), rec._3)
            }

          case None =>
            logger.debug(s"Ignored non-bid event")
        }

      case CHN_BROADCAST =>
        logger.debug(s"partition: $partition, received broadcast")
        try {
          val wd = readBinary[WindowDelta](rec._2)
          // only process deltas for the current queryId (used for chaining workloads)
          if (wd.queryId == queryId) {
            // merge each delta
            wd.deltas.foreach { db =>
              val d = readBinary[ReplicatedDelta](db)
              val (st, flag) = windowMap.getOrElse(wd.window, (crdt.empty(addr), false))
              windowMap(wd.window) = (crdt.mergeDelta(st, d), flag)
            }
            // merge vector clock
            vectorClock(wd.partition) = math.max(
              vectorClock(wd.partition), wd.vectorClock(wd.partition)
            )
            // mark final‐delta arrival
            if (wd.isFinal) {
              val pfMap = pendingFinal.getOrElseUpdate(wd.window, mutable.Map.empty)
              pfMap(wd.partition) = wd.vectorClock(wd.partition)
              // only flip the bit once we’ve actually merged up to that ts
              tryMarkFinal(wd.window)
            }
          }
        } catch {
          case e: Exception =>
            logger.debug(s"partition: $partition, error in CHN_BROADCAST: ${e.getMessage}")
        }

      case _ =>
        logger.debug(s"partition: $partition, Unknown channel: $chn")
        throw new RuntimeException(s"Unknown channel: $chn")
    }
  }

  private def tryMarkFinal(w: Long): Unit = {
    pendingFinal.get(w).foreach { pfMap =>
      for ((p, finalTs) <- pfMap.toList) {
        // only mark once our own vectorClock(p) has reached that final watermark ts
        if (vectorClock(p) >= finalTs) {
          ensureCompletionBits(w)
          completionBits(w).set(p)
          pfMap.remove(p)
        }
      }
      // clean up if empty
      if (pfMap.isEmpty) pendingFinal.remove(w)
    }
  }

  private def flushDeltas(outputFunction: (Int, Byte, LogProducerRecords) => Unit): Unit = {
    val toFlush = deltaMap.keys.toList
    toFlush.foreach { w =>
      val deltas = deltaMap(w)
      if (deltas.nonEmpty) {
        ensureCompletionBits(w)
        val msg = WindowDelta(partition, queryId, vectorClock.clone(), w, deltas.map(writeBinary), isFinal = false)
        outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(msg))))
        // only clear this window's list
        deltaMap(w) = Nil
      }
    }
  }

  private def flushFinalDelta(w: Long, outputFunction: (Int, Byte, LogProducerRecords) => Unit): Unit = {
    ensureCompletionBits(w)
    val pfMap = pendingFinal.getOrElseUpdate(w, mutable.Map.empty)
    pfMap(partition) = vectorClock(partition)
    // attempt to mark (won’t actually set until we’ve merged all our own deltas)
    tryMarkFinal(w)

    // Broadcast
    val deltas = deltaMap.getOrElse(w, Nil)
    val msg = WindowDelta(partition, queryId, vectorClock.clone(), w, deltas.map(writeBinary), isFinal = true)
//    if USE_LOG_FILE then outputLog.info(s"partition: $partition, flushing final delta for window: $w with vector clock: ${vectorClock.mkString(",")}, deltas: ${deltas.mkString(",")}")
    outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(msg))))
    // only clear w
    deltaMap.remove(w)
  }

  // initialize per-window BitSet for tracking completion of windows across all partitions
  private def ensureCompletionBits(w: Long): Unit = {
    if (!completionBits.contains(w)) {
      val bs = new java.util.BitSet(nrOfKafkaPartitions())
      completionBits(w) = bs
    }
  }

  // expose completeness for holon.streaming.windowing.WindowedQueryFun
  def isWindowComplete(w: Long): Boolean =
    // outputLog.info(s"partition: $partition, checking completion for window: $w, completionBits: ${completionBits.getOrElse(w, new java.util.BitSet(nrOfKafkaPartitions()))}, nrOfKafkaPartitions: ${nrOfKafkaPartitions()}")
    completionBits.get(w).exists(_.cardinality == nrOfKafkaPartitions())

  override def snapshot(): Array[Byte] = {
    logger.debug("Taking snapshot")
    val snapshotState = SnapShotState(emittedWindows, vectorClock, windowMap)
    writeBinary(snapshotState)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    logger.debug("Restoring from snapshot")
    if (snapshot != null) {
      val restored = readBinary[SnapShotState[T]](snapshot)
      windowMap.clear(); windowMap ++= restored.windowMap
      vectorClock = restored.vectorClock
      emittedWindows = restored.emittedWindows
    }
  }

  def garbageCollect(windowKey: Long): Unit = {
    logger.debug(s"partition: $partition, GC window: $windowKey")
    windowMap.remove(windowKey)
    deltaMap.remove(windowKey)
    completionBits.remove(windowKey)
    lagAppendTimePerWindow.remove(windowKey)
  }

  // Used for the taxi dataset
  // def defineWindow(eventTime: Long): Long = {
  //   val startTime = LocalDateTime.parse("2013-01-01T00:00:00").atZone(java.time.ZoneId.of("UTC")).toInstant.toEpochMilli
  //   val windowSizeMillis = 15 * 60 * 1000 // 15 minutes in milliseconds
  //   (eventTime - startTime) / windowSizeMillis
  // }

  def defineWindow(eventTime: Long): Long = {
   val time = eventTime / 10
   if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH else (time / WINDOW_LENGTH) + 1
 }
}
