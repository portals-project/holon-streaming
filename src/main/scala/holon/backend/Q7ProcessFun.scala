package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import holon.crdt.{CRDTWrapper, HighestBidLWWRegisterWrapper}
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
import org.apache.pekko.cluster.ddata.LWWRegister
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

// Count total amount of bids
class Q7ProcessFun(partition: Int) extends ProcFun {
  // Set up logger
  private val logger = Logger("Q7ProcessFun")
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  Logger.setLevel("Q7ProcessFun", "INFO")
  logger.info("Starting Q7ProcessFun")

  // First wrapper maps auctions to their highest bid
  val w0: CRDTWrapper[LWWRegister[Array[Byte]], String] = HighestBidLWWRegisterWrapper
  val highestBid = new WindowedRecordProcFun[LWWRegister[Array[Byte]], String](w0, partition, 0, lwwRegisterBytesRW)

  var procfuns: List[WindowedRecordProcFun[LWWRegister[Array[Byte]], String]] = List(highestBid)

  logger.debug(s"Set up procfuns: $procfuns")

  // Holds the latest window key that has been queried and processed.
  var queriedWindow: Long = 1L
  // Holds the last closed window key.
  var lastClosedWindow: Long = 0L

  val defineWindow = highestBid.defineWindow

  def processInput(
                      outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                 chn: Byte,
                 rec: LogConsumerRecords,
                  ): Unit = {
    val inputRecords = rec
    val out0: Unit = highestBid.processInput(outputFunction, chn, inputRecords)

    // Get the minimum vector clock value from both queries
    val minVC: Array[Long] = highestBid.vectorClock

    logger.debug(s"partition: $partition minVC: ${minVC.mkString("Array(", ", ", ")")}")

    // Start processing windows if vc is not empty
    if !minVC.contains(0) then
      // Get the minimum vector clock value across all partitions
      lastClosedWindow = defineWindow(minVC.min) - 1L
    else
      lastClosedWindow = -1L

    if (lastClosedWindow > 0) {
      for (i <- queriedWindow until lastClosedWindow) {
        logger.debug(s"partition: $partition processing window: $i with lastClosedWindow: $lastClosedWindow")

        if highestBid.windowMap.contains(i)then
          val result: String = w0.value(highestBid.windowMap(i)._1)
          val outputState = OutputState(partition, i, result)

          if (highestBid.logAppendTimePerWindow.contains(i)) {
            logger.info(s"[LagAppendInput] - query 0 - window: $i, timestamp: ${highestBid.logAppendTimePerWindow(i)}")
          }

          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](outputState))))

          // Garbage collect the window
          highestBid.garbageCollect(i)
          queriedWindow = i
      }
    }
  }

  def snapshot(): Array[Byte] = {
    // Snapshot each procFun in the list
    writeBinary(procfuns.map(_.snapshot()))
  }

  def restore(allBytes: Array[Byte]): Unit = {
    // read back the List[Array[Byte]]
    val snaps: List[Array[Byte]] = readBinary[List[Array[Byte]]](allBytes)

    // Restore each procFun in the listc
    snaps.zip(procfuns).foreach { case (bytes, pf) =>
      pf.restore(bytes)
    }
  }
}