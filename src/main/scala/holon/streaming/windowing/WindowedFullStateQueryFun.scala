package holon.streaming.windowing

import holon.core.Config.{CHN_OUTPUT, GARBAGE_COLLECTION_OFFSET, USE_LOG_FILE}
import holon.utils.{LogConsumerRecords, LogProducerRecords, Logger}
import holon.streaming.processing.ProcFun
import holon.crdt.serialization.WindowStateSerialization.OutputState
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

abstract class WindowedFullStateQueryFun(partition: Int, val procfuns: List[WindowedRecordProcFunFullState[_,_]]) extends ProcFun {

  private var queriedWindow = 1L

  private val firstProcFun = procfuns.head

  private val defineWindow = firstProcFun.defineWindow

  val logger = Logger.apply("holon.streaming.windowing.WindowedQueryFun")
  val outputLog = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("holon.streaming.windowing.WindowedQueryFun", "INFO")

  final override def processInput(
                                   outputFun: (Int, Byte, LogProducerRecords)=>Unit,
                                   chn: Byte,
                                   recs: LogConsumerRecords
                                 ): Unit = {
    for record <- recs do
      procfuns.foreach(_.processInput(outputFun, chn, Iterable(record)))

      val minVC = procfuns.map(_.vectorClock).reduce { (a, b) => a.zip(b).map { case (x,y) => math.min(x,y) } }

      // check only the local logical clock, no broadcast for Q0
      val lastClosed = defineWindow(minVC(partition)) - 1L

      if (lastClosed > queriedWindow) {
        // no need to check isWindowComplete here, since we are not using broadcast
        for (w <- queriedWindow until lastClosed) {
          val crdtStates = procfuns.map(_.windowMap(w)._1)
          val out: OutputState = OutputState(partition, w, processWindow(w, crdtStates))

          if USE_LOG_FILE then
            if firstProcFun.lagAppendTimePerWindow.contains(w) then
              outputLog.info(s"[LagAppendInput] - window: $w, timestamp: ${firstProcFun.lagAppendTimePerWindow(w)}")

              // optional: Log state size
              // val serializedState: Array[Byte] = writeBinary(firstProcFun.windowMap)
              // outputLog.info(s"[STATE-SIZE]: partition: $partition, size: ${serializedState.length}, timestamp: ${System.currentTimeMillis()}")
          else
            if firstProcFun.lagAppendTimePerWindow.contains(w) then
              logger.info(s"[LagAppendInput] - window: $w, timestamp: ${firstProcFun.lagAppendTimePerWindow(w)}")

              // optional: Log state size
              // val serializedState: Array[Byte] = writeBinary(firstProcFun.windowMap)
              // logger.info(s"[STATE-SIZE]: partition: $partition, size: ${serializedState.length}, timestamp: ${System.currentTimeMillis()}")

          outputFun(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](out))))
        }
        queriedWindow = lastClosed

        // garbage collection
        procfuns.foreach { pf =>
          pf.windowMap.keys
            .filter(_ <= lastClosed - GARBAGE_COLLECTION_OFFSET)
            .foreach(pf.garbageCollect)
        }
      }
  }

  protected def processWindow(window: Long, states: List[Any]): String

  def snapshot(): Array[Byte] = {
    logger.info(s"partition: $partition, snapshotting with: queriedWindow: $queriedWindow")
    // snapshot each procFun in the list
    val snaps: List[Array[Byte]] = writeBinary(queriedWindow) :: procfuns.map(_.snapshot())
    writeBinary(snaps)
  }

  def restore(allBytes: Array[Byte]): Unit = {
    val snaps: List[Array[Byte]] = readBinary[List[Array[Byte]]](allBytes)
    queriedWindow = readBinary[Long](snaps.head)

    // restore each procFun in the list
    for ((procFun, index) <- snaps.tail.zipWithIndex) {
      procfuns(index).restore(procFun)
    }
    logger.info(s"partition: $partition, restored with queriedWindow: $queriedWindow")
  }
}