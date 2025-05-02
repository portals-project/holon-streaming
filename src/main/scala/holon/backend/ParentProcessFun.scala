package holon.backend

import holon.Config.CHN_OUTPUT
import holon.crdt.{AuctionToCategoryWrapper, AuctionToHighestBidWrapper, CRDTWrapper}
import holon.*
import org.apache.pekko.cluster.ddata.GSet
import upickle.legacy.{ReadWriter, readBinary, readwriter, writeBinary}
import holon.serialization.*

// Nexmark Query 4:
// Select the average of the wining bid prices for all auctions in each category.
class ParentProcessFun(partition: Int) extends ProcFun {
  // Set up logger
  private val logger = Logger("ParentProcessFun")
  Logger.setLevel("ParentProcessFun", "INFO")
  logger.info("Starting ParentProcessFun")

  // First wrapper maps auctions to their highest bid
  val w0: CRDTWrapper[GSet[(Long, Long)], java.util.Set[(Long, Long)]] = AuctionToHighestBidWrapper
  val auctionToBids = new WindowedRecordProcFun[GSet[(Long, Long)], java.util.Set[(Long, Long)]](w0, partition, 0, rwTuple, javaSetReadWriter)

  // Second wrapper maps auctions to their category
  val w1: CRDTWrapper[GSet[(Long, Long)], java.util.Set[(Long, Long)]] = AuctionToCategoryWrapper
  val auctionsToCats = new WindowedRecordProcFun[GSet[(Long, Long)], java.util.Set[(Long, Long)]](w1, partition, 1, rwTuple, javaSetReadWriter)

  var procfuns: List[WindowedRecordProcFun[GSet[(Long,Long)], java.util.Set[(Long, Long)]]] = List(auctionToBids, auctionsToCats)

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
    val defineWindow = auctionToBids.defineWindow
    val out0 = auctionToBids.processInput(outputFunction, chn, inputRecords)
    val out1 = auctionsToCats.processInput(outputFunction, chn, inputRecords)

    // Get the latest vector clock from both queries
    val aucToBidVC = auctionToBids.vectorClock
    val aucToCatVC = auctionsToCats.vectorClock

    logger.debug(s"aucToBidVC: ${aucToBidVC.mkString("Array(", ", ", ")")}")
    logger.debug(s"aucToCatVC: ${aucToCatVC.mkString("Array(", ", ", ")")}")

    // Get the minimum vector clock value from both queries
    val minVC: Array[Long] = aucToBidVC.zip(aucToCatVC).map { case (v0, v1) => math.min(v0, v1) }

    logger.debug(s"partition: $partition minVC: ${minVC.mkString("Array(", ", ", ")")}")

    // Start processing windows if vc is not empty
    if !minVC.contains(0) then
      // Get the minimum vector clock value across all partitions
      lastClosedWindow = auctionToBids.defineWindow(minVC.filter(_ > 0).min) - 1L
    else
      lastClosedWindow = -1L

    if (lastClosedWindow > 0) {
      for (i <- queriedWindow until lastClosedWindow) {
        logger.debug(s"partition: $partition processing window: $i")

        val result = processWindow(auctionToBids.windowMap(i)._1, auctionsToCats.windowMap(i)._1, i)
        val outputState = OutputState(partition, i, result.toString())
        outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](outputState))))
      }
      queriedWindow = lastClosedWindow
    }

    def processWindow(bidsSet: GSet[(Long, Long)], catsSet: GSet[(Long, Long)], winKey: Long): Map[Long, Double] = {
      // 1. Raw bids and categories
      val bids: Set[(Long,Long)] = bidsSet.elements
      val catsByAuction: Map[Long,Set[Long]] =
        catsSet.elements.groupMap(_._1)(_._2)

      // 2. Max bid per auction
      val maxBidPerAuction: Map[Long, Long] =
        bids.groupMap(_._1)(_._2).view.mapValues(_.max).toMap

      // 3) Create a flat list of (catId, maxBid) pairs
      val catBidPairs: Seq[(Long, Long)] =
        maxBidPerAuction.toSeq.flatMap { case (aid, maxBid) =>
          catsByAuction.getOrElse(aid, Set.empty).map(catId => (catId, maxBid))
        }

      logger.debug(s"window: $winKey, flat pairs: ${catBidPairs.mkString(", ")}")

      // 4. Group by category into Seq of bids (including all max bids for each auction)
      val bidsByCat: Map[Long, Seq[Long]] =
        catBidPairs.groupMap(_._1)(_._2)

      logger.debug(s"window: $winKey, bidsByCat: $bidsByCat")

      // 5. Compute average per category
      val avgByCat: Map[Long, Double] =
        bidsByCat.view.mapValues { prices =>
          prices.sum.toDouble / prices.size
        }.toMap

      logger.debug(s"partition: $partition window: $winKey, avgByCat: $avgByCat")

      avgByCat
    }
  }

  def snapshot(): Array[Byte] = {
    // Snapshot each procFun in the list
    writeBinary(procfuns.map(_.snapshot()))
  }

  def restore(allBytes: Array[Byte]): Unit = {
    // read back the List[Array[Byte]]
    val snaps: List[Array[Byte]] = readBinary[List[Array[Byte]]](allBytes)

    // Restore each procFun in the list
    snaps.zip(procfuns).foreach { case (bytes, pf) =>
      pf.restore(bytes)
    }
  }
}