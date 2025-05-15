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

  var procfuns: List[WindowedRecordProcFun[_, _]] = List(highestBid)

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
    logger.debug(s"partition: $partition, processing input records: ${inputRecords.size}")
    val highestBidOutput: highestBid.type = procfuns.head.asInstanceOf[highestBid.type]
    for record <- inputRecords do
      val iterable: Iterable[(Array[Byte], Array[Byte], Long)] = Iterable(record)

      highestBidOutput.processInput(outputFunction, chn, iterable)

      // Get the minimum vector clock value from both queries
      val minVC: Array[Long] = highestBidOutput.vectorClock

      logger.debug(s"partition: $partition minVC: ${minVC.mkString("Array(", ", ", ")")}")

      // Start processing windows if vc is not empty
      if !minVC.contains(0) then
        // Get the minimum vector clock value across all partitions
        lastClosedWindow = defineWindow(minVC.min) - 1L
      else
        lastClosedWindow = -1L

      if (lastClosedWindow > queriedWindow) {
        for (i <- queriedWindow until lastClosedWindow if highestBidOutput.windowMap.contains(i)) {
          val result = w0.value(highestBidOutput.windowMap(i)._1)
          val outputState = OutputState(partition, i, result)

          if (highestBid.logAppendTimePerWindow.contains(i)) {
            logger.info(s"[LagAppendInput] - window: $i, ts: ${highestBid.logAppendTimePerWindow(i)}")
          }

          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](outputState))))
          highestBidOutput.garbageCollect(i)
        }
        queriedWindow = lastClosedWindow
      }
  }

  def snapshot(): Array[Byte] = {
    logger.info(s"partition: $partition, snapshotting Q7ProcessFun with: queriedWindow: $queriedWindow")
    // Snapshot each procFun in the list
    val snaps: List[Array[Byte]] = writeBinary(queriedWindow) :: procfuns.map(_.snapshot())
    writeBinary(snaps)
  }

  def restore(allBytes: Array[Byte]): Unit = {
    logger.info(s"partition: $partition, restoring Q7ProcessFun")

    val snaps: List[Array[Byte]] = readBinary[List[Array[Byte]]](allBytes)
    queriedWindow = readBinary[Long](snaps.head)

    for ((procFun, index) <- snaps.tail.zipWithIndex) {
      procfuns(index).restore(procFun)
    }
    logger.info(s"partition: $partition, restored Q7ProcessFun with: queriedWindow: $queriedWindow")
  }
}