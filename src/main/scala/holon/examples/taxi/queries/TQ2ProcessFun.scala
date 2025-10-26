package holon.examples.taxi.queries

import holon.utils.Logger
import holon.crdt.HighestBidLWWMapWrapper
import holon.streaming.windowing.{WindowedRecordProcFun, WindowedQueryFun}
import org.apache.pekko.cluster.ddata.LWWMap

class TQ2ProcessFun (partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
  private val logger = Logger("TQ2ProcessFun")
  Logger.setLevel("TQ2ProcessFun", "INFO")
  logger.info("Starting TQ2ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    // states.head is LWWRegister[Array[Byte]]
    HighestBidLWWMapWrapper.value(states.head.asInstanceOf[LWWMap[String, Array[Byte]]])
  }
}
