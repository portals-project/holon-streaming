package holon.backend

import holon.Config.CHN_OUTPUT
import holon.crdt.{AuctionToCategoryWrapper, AuctionToHighestBidWrapper, CRDTWrapper}
import holon.*
import org.apache.pekko.cluster.ddata.{GSet, LWWMap}
import upickle.legacy.{readBinary, writeBinary}
import holon.serialization.rwTuple
import holon.serialization.rwLWWMap
import org.slf4j.LoggerFactory

// Nexmark Query 4:
// Select the average of the wining bid prices for all auctions in each category.
class Q4ProcessFun(partition: Int) extends ProcFun {
  // Set up logger
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  private val systemOutputLog = LoggerFactory.getLogger("com.holon.system.output")
  private val logger = Logger("Q4ProcessFun")
  Logger.setLevel("Q4ProcessFun", "INFO")
  logger.info("Starting Q4ProcessFun")

  // First wrapper maps auctions to their highest bid
  val w0: CRDTWrapper[LWWMap[Long, Long], Map[Long, Long]] = AuctionToHighestBidWrapper
  val auctionToBids: WindowedRecordProcFun[LWWMap[Long, Long], Map[Long, Long]] = new WindowedRecordProcFun[LWWMap[Long, Long], Map[Long, Long]](w0, partition, 0, rwLWWMap)

  // Second wrapper maps auctions to their category
  val w1: CRDTWrapper[GSet[(Long, Long)], java.util.Set[(Long, Long)]] = AuctionToCategoryWrapper
  val auctionToCats: WindowedRecordProcFun[GSet[(Long, Long)], java.util.Set[(Long, Long)]] = new WindowedRecordProcFun[GSet[(Long, Long)], java.util.Set[(Long, Long)]](w1, partition, 1, rwTuple)

  var procfuns: List[WindowedRecordProcFun[_, _]] = List(auctionToBids, auctionToCats)

  logger.debug(s"Set up procfuns: $procfuns")

  // Holds the latest window key that has been queried and processed.
  var queriedWindow: Long = 1L
  // Holds the last closed window key.
  var lastClosedWindow: Long = 0L

  val defineWindow: Long => Long = auctionToBids.defineWindow

  def processInput(
                    outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                    chn: Byte,
                    rec: LogConsumerRecords,
                  ): Unit = {
    val inputRecords = rec
    val auctionToBidsOutput: auctionToBids.type = procfuns.head.asInstanceOf[auctionToBids.type]
    val auctionToCatsOutput: auctionToCats.type = procfuns.tail.head.asInstanceOf[auctionToCats.type]

    for record <- inputRecords do
      val iterable: Iterable[(Array[Byte], Array[Byte], Long)] = Iterable(record)

      // Process the input records
      auctionToBidsOutput.processInput(outputFunction, chn, iterable)
      auctionToCatsOutput.processInput(outputFunction, chn, iterable)

      // Get the latest vector clock from both queries
      val aucToBidVC = auctionToBidsOutput.vectorClock
      val aucToCatVC = auctionToCatsOutput.vectorClock

      logger.debug(s"aucToBidVC: ${aucToBidVC.mkString("Array(", ", ", ")")}")
      logger.debug(s"aucToCatVC: ${aucToCatVC.mkString("Array(", ", ", ")")}")

      // Get the minimum vector clock value from both queries
      val minVC: Array[Long] = aucToBidVC.zip(aucToCatVC).map { case (v0, v1) => math.min(v0, v1) }

      logger.debug(s"partition: $partition minVC: ${minVC.mkString("Array(", ", ", ")")}")

      // Start processing windows if vc is not empty
      if !minVC.contains(0) then
        // Get the minimum vector clock value across all partitions
        lastClosedWindow = defineWindow(minVC.min) - 1L
      else
        lastClosedWindow = -1L

      if (lastClosedWindow > queriedWindow) {
        for (i <- queriedWindow until lastClosedWindow if auctionToBidsOutput.windowMap.contains(i) && auctionToCatsOutput.windowMap.contains(i)) {
          logger.debug(s"partition: $partition processing window: $i with lastClosedWindow: $lastClosedWindow")

          val result = processWindow(auctionToBidsOutput.windowMap(i)._1, auctionToCatsOutput.windowMap(i)._1, i)
          val outputState = OutputState(partition, i, result.toString())

          if (auctionToBidsOutput.logAppendTimePerWindow.contains(i)) {
            logger.info(s"[LagAppendInput] - query 0 - window: $i, timestamp: ${auctionToBidsOutput.logAppendTimePerWindow(i)}")
          }

          outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary[OutputState](outputState))))

          // Garbage collect the window
          auctionToBidsOutput.garbageCollect(i)
          auctionToCatsOutput.garbageCollect(i)

        }
        queriedWindow = lastClosedWindow
      }
  }

  def processWindow(bidsMap: LWWMap[Long, Long], catsSet: GSet[(Long, Long)], winKey: Long): Map[Long, Double] = {
    logger.debug(s"partition: $partition processing window: $winKey, bidsMap: $bidsMap, catsSet: $catsSet")

    // 1. Extract the one-entry-per-auction bid map
    val bids: Map[Long, Long] = bidsMap.entries

    // 2. Build auction→categories mapping
    val catsByAuction: Map[Long, Set[Long]] =
      catsSet.elements.groupMap(_._1)(_._2)

    // 3. Emit (category, bid) pairs by looking up each auction’s bid
    val catBidPairs: Seq[(Long, Long)] =
      bids.toSeq.flatMap { case (auctionId, bidPrice) =>
        catsByAuction.getOrElse(auctionId, Set.empty).map(catId => (catId, bidPrice))
      }

    logger.debug(s"window: $winKey, flat pairs: ${catBidPairs.mkString(", ")}")

    // 4. Group by category
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

  def snapshot(): Array[Byte] = {
    logger.info(s"partition: $partition, snapshotting Q4ProcessFun with: queriedWindow: $queriedWindow")
    // Snapshot each procFun in the list
    val snaps: List[Array[Byte]] = writeBinary(queriedWindow) :: procfuns.map(_.snapshot())
    writeBinary(snaps)
  }

  def restore(allBytes: Array[Byte]): Unit = {
    logger.info(s"partition: $partition, restoring Q4ProcessFun")

    val snaps: List[Array[Byte]] = readBinary[List[Array[Byte]]](allBytes)
    queriedWindow = readBinary[Long](snaps.head)

    for ((procFun, index) <- snaps.tail.zipWithIndex) {
      procfuns(index).restore(procFun)
    }
    logger.info(s"partition: $partition, restored Q4ProcessFun with: queriedWindow: $queriedWindow")
  }
}