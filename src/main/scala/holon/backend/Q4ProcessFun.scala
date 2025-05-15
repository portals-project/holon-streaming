package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import org.apache.pekko.cluster.ddata.{GSet, LWWMap}
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

// Nexmark Q4: avg winning bid per category
class Q4ProcessFun(partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
  private val logger = Logger("Q4ProcessFun")
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("Q4ProcessFun", "INFO")
  logger.info("Starting Q4ProcessFun")
  
  override protected def processWindow(window: Long, states: List[Any]): String = {
    logger.debug(s"partition=$partition processWindow $window")
    
    val bidsMap = states(0).asInstanceOf[LWWMap[Long, Long]].entries
    val catsSet = states(1).asInstanceOf[GSet[(Long, Long)]].elements
    
    val catsByAuction: Map[Long, Set[Long]] =
      catsSet.groupMap(_._1)(_._2).view.mapValues(_.toSet).toMap
    
    val catBidPairs: Seq[(Long, Long)] =
      bidsMap.toSeq.flatMap { case (auc, bid) =>
        catsByAuction.getOrElse(auc, Set.empty).map(cat => (cat, bid))
      }
    
    val bidsByCat: Map[Long, Seq[Long]] =
      catBidPairs.groupMap(_._1)(_._2)
    
    val avgByCat: Map[Long, Double] =
      bidsByCat.view.mapValues { bids =>
        bids.sum.toDouble / bids.size
      }.toMap

    logger.debug(s"partition=$partition window=$window avgByCat=$avgByCat")
    avgByCat.toString()
  }
}
