package holon.backend

import holon.Config.{CHN_OUTPUT, GARBAGE_COLLECTION_OFFSET}
import holon.{LogConsumerRecords, LogProducerRecords, Logger, ProcFun}
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

import scala.collection.mutable

abstract class WindowedQueryFun(partition: Int, val procfuns: List[WindowedRecordProcFun[_,_]]) extends ProcFun {

  private var queriedWindow = 1L

  private val firstProcFun = procfuns.head

  private val defineWindow = firstProcFun.defineWindow

  val logger = Logger.apply("WindowedQueryFun")
  val outputLog = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("WindowedQueryFun", "INFO")

  final override def processInput(
                                   outputFun: (Int, Byte, LogProducerRecords)=>Unit,
                                   chn: Byte,
                                   recs: LogConsumerRecords
                                 ): Unit = {
    // TODO: see if the loop so be so deep
    for record <- recs do
      // Step 4: Process the input records for each procfun
      procfuns.foreach(_.processInput(outputFun, chn, Iterable(record)))

      // Step 1: Compute the min vector-clock
      val minVC = procfuns.map(_.vectorClock).reduce { (a, b) => a.zip(b).map { case (x,y) => math.min(x,y) } }

      // Step 2: Find the last closed window
      val lastClosed = if (!minVC.contains(0L)) defineWindow(minVC.min) - 1L else -1L

      // Step 3: For each unprocessed window, process the states and output them
      // TODO: Check if this logic is causing inconsistent results
      if (lastClosed > queriedWindow) {
//        outputLog.info(s"[WindowedQueryFun] - partition: $partition, found newly non emitted windows, lastClosed: $lastClosed, queriedWindow: $queriedWindow, this is the vector clock: ${minVC.mkString(",")}")
        for (w <- queriedWindow until lastClosed if procfuns.forall(_.windowMap.contains(w))) {
//          outputLog.info(s"[WindowedQueryFun] - partition: $partition, processing window: $w with this vector clock: ${minVC.mkString(",")}")
          // collect the raw CRDT states
          val crdtStates = procfuns.map(_.windowMap(w)._1)
//          outputLog.info(s"[WindowedQueryFun] - partition: $partition, collected CRDT states for window: $w")
          val out: OutputState = OutputState(partition, w, processWindow(w, crdtStates))

          if firstProcFun.logAppendTimePerWindow.contains(w) then logger.info(s"[LagAppendInput] - window: $w, timestamp: ${firstProcFun.logAppendTimePerWindow(w)}")

//          outputLog.info(s"[WindowedQueryFun] - partition: $partition, emitting final value for window: $w, output: ${out.value} with vector clock: ${minVC.mkString(",")}")
          outputFun(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](out))))
        }
        queriedWindow = lastClosed

        // Garbage collection
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
    // Snapshot each procFun in the list
    val snaps: List[Array[Byte]] = writeBinary(queriedWindow) :: procfuns.map(_.snapshot())
    writeBinary(snaps)
  }

  def restore(allBytes: Array[Byte]): Unit = {
    val snaps: List[Array[Byte]] = readBinary[List[Array[Byte]]](allBytes)
    queriedWindow = readBinary[Long](snaps.head)

    for ((procFun, index) <- snaps.tail.zipWithIndex) {
      procfuns(index).restore(procFun)
    }
    logger.info(s"partition: $partition, restored with queriedWindow: $queriedWindow")
  }
}