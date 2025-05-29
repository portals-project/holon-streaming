package holon.backend

import holon.*
import holon.crdt.CRDTWrapper
import holon.example.Nexmark
import holon.example.CRDT.address
import holon.Config.*
import org.apache.pekko.cluster.ddata.SelfUniqueAddress
import org.slf4j.LoggerFactory
import upickle.legacy.{ReadWriter, macroRW, readBinary, writeBinary}

import scala.collection.mutable

/** Message carrying the *entire* CRDT state for one window. */
case class WindowFullState[T](
                               partition:   Int,
                               queryId:     Int,
                               vectorClock: Array[Long],
                               window:      Long,
                               state:       T,
                               isFinal:     Boolean
                             )
object WindowFullState {
  given [T: ReadWriter]: ReadWriter[WindowFullState[T]] = macroRW
}

/** Full-state version of your windowed ProcFun (Q0). */
case class WindowedRecordProcFunFullState[T, V](
                                                 crdt:      CRDTWrapper[T, V],
                                                 partition: Int,
                                                 queryId:   Int,
                                                 rw:        ReadWriter[T]
                                               ) extends ProcFun {
  private val logger       = Logger("WindowedRecordProcFunFullState")
  private val outputLog    = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("WindowedRecordProcFunFullState", "INFO")
  logger.info(s"Starting WindowedRecordProcFunFullState for partition: $partition")
  import WindowFullState.given

  // for upickle when we serialize T inside WindowFullState
  implicit val elementRW: ReadWriter[T] = rw

  private val addr             : SelfUniqueAddress            = address(partition)
  private val flushThreshold               = FLUSH_THRESHOLD
  private var eventsSinceLastFlush         = 0

  // per-window raw CRDT state and "closed?" flag
  val windowMap = mutable.Map.empty[Long, (T, Boolean)]
  private var queriedWindow = 1L
  var vectorClock = Array.fill(nrOfKafkaPartitions())(0L)
  private val pendingFinal  = mutable.Map.empty[Long, mutable.Map[Int, Long]]
  private val completionBits = mutable.Map.empty[Long, java.util.BitSet]
  val logAppendTimePerWindow = mutable.Map.empty[Long, Long]

  override def processInput(
                             outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                             chn:            Byte,
                             recs:           LogConsumerRecords
                           ): Unit = {
    val rec = recs.head
    chn match {
      case CHN_INPUT =>
        // 1) deserialize & filter
        val tsEvent = Nexmark.deserialize(rec._2)
        crdt.checkType(tsEvent).foreach { event =>
          val ts = crdt.timeStamp(event)
          val window = defineWindow(ts)

          // 2) init window state if needed
          if (!windowMap.contains(window)) {
            windowMap(window) = (crdt.empty(addr), false)
          }

          // 3) full‐state update (no deltas used)
          val (oldState, closed) = windowMap(window)
          val newState = crdt.update(oldState, addr, event)
          windowMap(window) = (newState, closed)

          // 4) periodic full‐state flush
          eventsSinceLastFlush += 1
          if (eventsSinceLastFlush >= flushThreshold) {
            flushFullStates(outputFunction, isFinal = false)
            eventsSinceLastFlush = 0
          }

          // 5) advance vector clock & detect local window‐close
          vectorClock(partition) = math.max(vectorClock(partition), ts)
          pendingFinal.keys.foreach(tryMarkFinal)

          val locallyClosed = defineWindow(vectorClock(partition)) - 1
          for (w <- queriedWindow until locallyClosed
             if !windowMap.get(w).exists(_._2)) {
                // a) ensure a state exists (empty if needed) & mark closed
                val st = windowMap.get(w).map(_._1).getOrElse(crdt.empty(addr))
                windowMap(w) = (st, true)

                // b) self‐mark completion bit BEFORE broadcast
                val pfMap = pendingFinal.getOrElseUpdate(w, mutable.Map.empty)
                pfMap(partition) = vectorClock(partition)
                val bs = completionBits.getOrElseUpdate(
                  w, new java.util.BitSet(nrOfKafkaPartitions())
                )
                if (vectorClock(partition) >= pfMap(partition)) {
                  bs.set(partition)
                }
                // c) broadcast final full‐state for window w
                flushFullStates(outputFunction, isFinal = true, targetWindow = w)
          }
          queriedWindow = locallyClosed

          // benchmark logging
          if (queryId == 0) {
            logAppendTimePerWindow(window) =
              math.max(logAppendTimePerWindow.getOrElse(window, 0L), rec._3)
          }
        }

      case CHN_BROADCAST =>
        // 6) on receipt, merge the remote full‐state
        try {
          val msg = readBinary[WindowFullState[T]](rec._2)
          if (msg.queryId == queryId) {
            // merge state
            val (cur, closed) = windowMap.getOrElse(msg.window, (crdt.empty(addr), false))
            val merged        = crdt.merge(cur, msg.state)
            windowMap(msg.window) = (merged, closed)

            // merge vector clock
            vectorClock(msg.partition) =
              math.max(vectorClock(msg.partition), msg.vectorClock(msg.partition))

            // treat final flag
            if (msg.isFinal) {
              val pfMap = pendingFinal.getOrElseUpdate(msg.window, mutable.Map.empty)
              pfMap(msg.partition) = msg.vectorClock(msg.partition)
              tryMarkFinal(msg.window)
            }
          }
        } catch {
          case _: Throwable =>
            logger.error(s"Failed to deserialize WindowFullState")
        }
      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }
  }

  /** Broadcasts full‐state for all windows (or one if targetWindow set). */
  private def flushFullStates(
                               output: (Int, Byte, LogProducerRecords) => Unit,
                               isFinal: Boolean,
                               targetWindow: Long = Long.MinValue): Unit = {
    val toFlush = if (targetWindow != Long.MinValue) Seq(targetWindow)
    else windowMap.keys.toSeq

    // Wherever a window is closed, emit state size
    val serializedState: Array[Byte] = writeBinary(windowMap)
    outputLog.info(s"[STATE-SIZE]: partition: $partition, size: ${serializedState.length}, timestamp: ${System.currentTimeMillis()}")

    toFlush.foreach { w =>
      val (state, _) = windowMap(w)
      val msg = WindowFullState(partition, queryId, vectorClock.clone(), w, state, isFinal)
      output(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), writeBinary(msg))))
    }
  }

  private def tryMarkFinal(w: Long): Unit = {
    pendingFinal.get(w).foreach { pfMap =>
      for ((p, finalTs) <- pfMap.toList) {
        if (vectorClock(p) >= finalTs) {
          val bs = completionBits.getOrElseUpdate(w, new java.util.BitSet(nrOfKafkaPartitions()))
          bs.set(p)
          pfMap.remove(p)
        }
      }
      if (pfMap.isEmpty) pendingFinal.remove(w)
    }
  }

  /** Same window‐numbering logic */
  def defineWindow(eventTime: Long): Long = {
    // Stream event time increases by 10
    val time = eventTime / 10
    if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH
    else (time / WINDOW_LENGTH) + 1
  }

  def isWindowComplete(w: Long): Boolean =
    completionBits.get(w).exists(_.cardinality == nrOfKafkaPartitions())

  def garbageCollect(windowKey: Long): Unit = {
    logger.debug(s"partition: $partition, GC window: $windowKey")
    windowMap.remove(windowKey)
    completionBits.remove(windowKey)
    logAppendTimePerWindow.remove(windowKey)
  }

  override def snapshot(): Array[Byte] = {
    // snapshot queriedWindow, vectorClock and windowMap
    val snapPart = writeBinary((queriedWindow, vectorClock))
    val windows  = windowMap.map { case (w, (s, _)) => (w, s) }.toMap
    val snapMap  = writeBinary(windows)
    writeBinary((snapPart, snapMap))
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    val (queried, vc) = readBinary[(Long, Array[Long])](snapshot)
    queriedWindow = queried
    vectorClock   = vc

    // restore windowMap
    val windows = readBinary[Map[Long, T]](snapshot.drop(writeBinary((queried, vc)).length))
    windowMap.clear()
    windows.foreach { case (w, s) => windowMap(w) = (s, false) }
  }
}
