package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import holon.crdt.*
import holon.serialization.mapGcounterRW
import org.apache.pekko.cluster.ddata.{GCounter, GSet, LWWRegister}
import upickle.legacy.{ReadWriter, readBinary, readwriter, writeBinary}

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

  def processInput(
                      outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                 chn: Byte,
                 rec: LogConsumerRecords,
                  ): Unit = {
    val inputRecords = rec
    val defineWindow = mostPopularAuction.defineWindow
    val out0 = mostPopularAuction.processInput(outputFunction, chn, inputRecords)

    // Get the minimum vector clock value from both queries
    val minVC: Array[Long] = mostPopularAuction.vectorClock

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

        val result: String = w0.value(mostPopularAuction.windowMap(i)._1)
        val outputState = OutputState(partition, i, result)
        outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](outputState))))
      }
      queriedWindow = lastClosedWindow
    }

//    def processWindow(counter: GCounter, winKey: Long): BigInt = {
//      // ...
//    }
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