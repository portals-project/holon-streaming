package holon.examples.nexmark.queries

import holon.utils.*
import holon.streaming.windowing.{WindowedRecordProcFun, WindowedQueryFun}
import org.apache.pekko.cluster.ddata.LWWMap
import holon.crdt.HighestBidLWWRegisterWrapper

// highest bid price
class Q7ProcessFun(partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
  private val logger = Logger("Q7ProcessFun")
  Logger.setLevel("Q7ProcessFun", "INFO")
  logger.info("Starting Q7ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    HighestBidLWWRegisterWrapper.value(states.head.asInstanceOf[LWWMap[String, Array[Byte]]])
  }
}