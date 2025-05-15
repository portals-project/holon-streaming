package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import holon.crdt.*
import holon.serialization.mapGcounterRW
import org.apache.pekko.cluster.ddata.GCounter
import upickle.legacy.{readBinary, writeBinary}

// Count total amount of bids
class Q5ProcessFun(partition: Int) extends ProcFun {
  // Set up logger
  private val logger = Logger("Q5ProcessFun")
  Logger.setLevel("Q5ProcessFun", "INFO")
  logger.info("Starting Q5ProcessFun")

  // First wrapper maps auctions to their highest bid
  val w0: CRDTWrapper[Map[String, GCounter], String] = AuctionGCounterWrapper
  val mostPopularAuction = new WindowedRecordProcFun[Map[String, GCounter], String](w0, partition, 0, mapGcounterRW)

  var procfuns: List[WindowedRecordProcFun[Map[String, GCounter], String]] = List(mostPopularAuction)

  logger.debug(s"Set up procfuns: $procfuns")

  // Holds the latest window key that has been queried and processed.
  var queriedWindow: Long = 1L
  // Holds the last closed window key.
  var lastClosedWindow: Long = 0L

  val defineWindow = mostPopularAuction.defineWindow

  def processInput(
                      outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                 chn: Byte,
                 rec: LogConsumerRecords,
                  ): Unit = {
    val inputRecords = rec
    logger.debug(s"partition: $partition, processing input records: ${inputRecords.size}")
    val mostPopularAuctionOutput: mostPopularAuction.type = procfuns.head.asInstanceOf[mostPopularAuction.type]

    for record <- inputRecords do
      val iterable: Iterable[(Array[Byte], Array[Byte], Long)] = Iterable(record)

      // Process the input records
      mostPopularAuctionOutput.processInput(outputFunction, chn, iterable)

      // Get the minimum vector clock value from both queries
      val minVC: Array[Long] = mostPopularAuctionOutput.vectorClock

      logger.debug(s"partition: $partition minVC: ${minVC.mkString("Array(", ", ", ")")}")

      // Start processing windows if vc is not empty
      if !minVC.contains(0) then
        // Get the minimum vector clock value across all partitions
        lastClosedWindow = defineWindow(minVC.min) - 1L
      else
        lastClosedWindow = -1L

      if (lastClosedWindow > queriedWindow) {
        for (i <- queriedWindow until lastClosedWindow if mostPopularAuctionOutput.windowMap.contains(i)) {
            val result: String = w0.value(mostPopularAuction.windowMap(i)._1)
            val outputState = OutputState(partition, i, result)

            if (mostPopularAuctionOutput.logAppendTimePerWindow.contains(i)) {
              logger.info(s"[LagAppendInput] - window: $i, timestamp: ${mostPopularAuctionOutput.logAppendTimePerWindow(i)}")
            }
            outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](outputState))))

            // Garbage collect the window
            mostPopularAuctionOutput.garbageCollect(i)
        }
        queriedWindow = lastClosedWindow
      }
  }

  def snapshot(): Array[Byte] = {
    logger.info(s"partition: $partition, snapshotting Q5ProcessFun with: queriedWindow: $queriedWindow")
    // Snapshot each procFun in the list
    val snaps: List[Array[Byte]] = writeBinary(queriedWindow) :: procfuns.map(_.snapshot())
    writeBinary(snaps)
  }

  def restore(allBytes: Array[Byte]): Unit = {
    logger.info(s"partition: $partition, restoring Q5ProcessFun")

    val snaps: List[Array[Byte]] = readBinary[List[Array[Byte]]](allBytes)
    queriedWindow = readBinary[Long](snaps.head)

    for ((procFun, index) <- snaps.tail.zipWithIndex) {
      procfuns(index).restore(procFun)
    }
    logger.info(s"partition: $partition, restored Q5ProcessFun with: queriedWindow: $queriedWindow")
  }
}