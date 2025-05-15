package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import holon.crdt.BidCountGCounterWrapper
import org.apache.pekko.cluster.ddata.GCounter

class BidCountProcessFun(partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
  private val logger = Logger("BidCountProcessFun")
  Logger.setLevel("BidCountProcessFun", "INFO")
  logger.info("Starting BidCountProcessFun")
  
  override protected def processWindow(window: Long, states: List[Any]): String = {
    val counter = states.head.asInstanceOf[GCounter]
    BidCountGCounterWrapper.value(counter)
  }
}
