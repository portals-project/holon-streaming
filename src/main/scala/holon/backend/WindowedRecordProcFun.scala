package holon.backend

import holon.*
import holon.crdt.CRDTWrapper
import holon.example.{CRDT, Nexmark}
import holon.example.CRDT.address
import holon.Config.*
import org.apache.pekko.cluster.ddata.{DeltaReplicatedData, ReplicatedDelta, SelfUniqueAddress}
import org.slf4j.LoggerFactory
import upickle.legacy.{ReadWriter, macroRW, readBinary, readwriter, writeBinary}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

// Generic implicit for mutable maps
implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[mutable.Map[K, V]] =
  readwriter[Map[K, V]].bimap[mutable.Map[K, V]](
    _.toMap,
    m => mutable.Map.empty[K, V] ++ m
  )

  // === custom ReadWriter for DeltaReplicatedData via Java serialization ===
implicit val deltaRW: ReadWriter[ReplicatedDelta] =
  readwriter[Array[Byte]].bimap[ReplicatedDelta](
    delta => {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(delta)
      oos.close()
      baos.toByteArray
    },
    bytes => {
      val bais = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bais)
      val obj = ois.readObject().asInstanceOf[ReplicatedDelta]
      ois.close()
      obj
    }
  )
// =====================================================================

case class WindowDelta(
                        partition: Int,
                        queryId:  Int,
                        vectorClock: Array[Long],
                        window: Long,
                        deltas: List[Array[Byte]],
                        isFinal: Boolean
                      )
object WindowDelta { given ReadWriter[WindowDelta] = macroRW }

// Not used anymore
case class WindowState[T](
                           partition: Int,
                           queryId: Int,
                           vectorClock: Array[Long],
                           windowMap: mutable.Map[Long, (T, Boolean)]
                         )
object WindowState { given windowStateRW[T: ReadWriter]: ReadWriter[WindowState[T]] = macroRW }

case class SnapShotState[T](
                             emittedWindows: Long,
                             vectorClock: Array[Long],
                             windowMap: mutable.Map[Long, (T, Boolean)]
                           )
object SnapShotState { given snapShotStateRW[T: ReadWriter]: ReadWriter[SnapShotState[T]] = macroRW }

case class OutputState(
                        partition: Int,
                        window: Long,
                        value: String
                      )

object OutputState {
  given ReadWriter[OutputState] = macroRW
}

// This processing function maps bids to auctions and aggregates per window.
case class WindowedRecordProcFun[T, V](crdt: CRDTWrapper[T, V], partition: Int, queryId: Int = 0, rw:ReadWriter[T]) extends ProcFun {
  private val logger = Logger("WindowedRecordProcFun")
  private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("WindowedRecordProcFun", "INFO")
  logger.info(s"Starting WindowedRecordProcFun for partition: $partition")

  // whenever you need an implicit ReadWriter[T], unwrap or throw
  implicit val elementRW: ReadWriter[T] = rw
  private val addr: SelfUniqueAddress = address(partition)

  var vectorClock: Array[Long] = Array.fill(nrOfKafkaPartitions())(0L)
  val windowMap = mutable.Map.empty[Long, (T, Boolean)]
  private var emittedWindows = 0L
  var queriedWindow: Long = 1L

  val logAppendTimePerWindow = mutable.Map.empty[Long, Long]

  // DELTA SUPPORT
  private val deltaMap = mutable.Map.empty[Long, List[ReplicatedDelta]]
  private val completionBits = mutable.Map.empty[Long, java.util.BitSet]
  private val flushThreshold = FLUSH_THRESHOLD
  private var eventsSinceLastFlush = 0
  // for each window w, a map from partition → its final water-mark ts
  private val pendingFinal = mutable.Map.empty[Long, mutable.Map[Int, Long]]

  override def processInput(
                             outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                             chn: Byte,
                             recs: LogConsumerRecords
                           ): Unit = {
    val rec = recs.head
    chn match {
      case CHN_INPUT =>
        val tsEvent = Nexmark.deserialize(rec._2)
        crdt.checkType(tsEvent) match {
          case Some(event) =>
            val eventTimestamp = crdt.timeStamp(event)
            val window = defineWindow(eventTimestamp)

            if (!windowMap.contains(window)) {
              logger.debug(s"partition: $partition Creating new window: $window")
              windowMap(window) = (crdt.empty(addr), false)
            }

            // 1) delta‐aware update
            val (updated, maybeDelta) = crdt.updateWithDelta(windowMap(window)._1, addr, event)
            windowMap(window) = (updated, windowMap(window)._2)

            // only if a delta actually exists do we store/broadcast it
            maybeDelta.foreach { delta =>
              deltaMap.update(window, delta :: deltaMap.getOrElse(window, Nil))
            }
            eventsSinceLastFlush += 1

            // 3) periodic flush of intermediate deltas
            if (eventsSinceLastFlush >= flushThreshold) {
              flushDeltas(outputFunction)
              eventsSinceLastFlush = 0
            }

            // 4) advance vector clock
            vectorClock(partition) = math.max(vectorClock(partition), eventTimestamp)

            pendingFinal.keys.foreach(tryMarkFinal)

            // 5) local window‐close detection & final‐delta
            val locallyClosed = defineWindow(vectorClock(partition)) - 1L
            if (locallyClosed > queriedWindow) {
              for (w <- queriedWindow until locallyClosed if !windowMap.get(w).exists(_._2)) {
                // if we had no entry, treat it as empty state
                val (st, _) = windowMap.getOrElse(w, (crdt.empty(addr), false))
                // mark closed & send final‐delta
                windowMap(w) = (st, true)
                flushFinalDelta(w, outputFunction)
              }
              queriedWindow = locallyClosed
            }

            // optional benchmark logging
            if (queryId == 0) {
              logAppendTimePerWindow(window) = math.max(logAppendTimePerWindow.getOrElse(window, 0L), rec._3)
            }

          case None =>
            logger.debug(s"Ignored non-bid event")
        }

      case CHN_BROADCAST =>
        logger.debug(s"partition: $partition, received broadcast")
        try {
          val wd = readBinary[WindowDelta](rec._2)
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
              // record “partition wd.partition says final @ this ts”
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
    // if nobody’s ever said final for w, nothing to do
    pendingFinal.get(w).foreach { pfMap =>
      // snapshot the entries to avoid concurrent‐mod issues
      for ((p, finalTs) <- pfMap.toList) {
        // only mark once our own vectorClock(p) has reached that ts
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

  /** Helper: broadcast all accumulated deltas (isFinal = false) */
  private def flushDeltas(outputFunction: (Int, Byte, LogProducerRecords) => Unit): Unit = {
    // snapshot the keys so we don't mutate while iterating
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

  /** Helper: broadcast final deltas for window w (isFinal = true) */
  private def flushFinalDelta(w: Long, outputFunction: (Int, Byte, LogProducerRecords) => Unit): Unit = {
    ensureCompletionBits(w)
    // 1) record “I (partition) say final @ this ts”
    val pfMap = pendingFinal.getOrElseUpdate(w, mutable.Map.empty)
    pfMap(partition) = vectorClock(partition)
    // 2) attempt to mark (won’t actually set until we’ve merged all our own deltas)
    tryMarkFinal(w)

    // Wherever a window is closed, emit state size
    val serializedState: Array[Byte] = writeBinary(windowMap)
    if USE_LOG_FILE then
      outputLog.info(s"[STATE-SIZE]: partition: $partition, size: ${serializedState.length}, timestamp: ${System.currentTimeMillis()}")
    else
      logger.info(s"[STATE-SIZE]: partition: $partition, size: ${serializedState.length}, timestamp: ${System.currentTimeMillis()}")

    // grab whatever's left for this window
    val deltas = deltaMap.getOrElse(w, Nil)
    val msg = WindowDelta(partition, queryId, vectorClock.clone(), w, deltas.map(writeBinary), isFinal = true)
    if USE_LOG_FILE then outputLog.info(s"partition: $partition, flushing final delta for window: $w with vector clock: ${vectorClock.mkString(",")}, deltas: ${deltas.mkString(",")}")
    outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(msg))))
    // now only clear w
    deltaMap.remove(w)
  }

  /** Initialize per-window BitSet */
  private def ensureCompletionBits(w: Long): Unit = {
    if (!completionBits.contains(w)) {
      val bs = new java.util.BitSet(nrOfKafkaPartitions())
      completionBits(w) = bs
    }
  }

  /** Expose completeness for WindowedQueryFun */
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
    logAppendTimePerWindow.remove(windowKey)
  }

  def defineWindow(eventTime: Long): Long = {
    val time = eventTime / 10
    if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH else (time / WINDOW_LENGTH) + 1
  }
}
