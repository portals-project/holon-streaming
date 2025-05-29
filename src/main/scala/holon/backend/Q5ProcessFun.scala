//package holon.backend
//
//import holon.*
//import holon.crdt.AuctionGCounterWrapper
//import org.apache.pekko.cluster.ddata.GCounter
//import org.slf4j.LoggerFactory
//import upickle.legacy.{readBinary, writeBinary}
//
//// “Most popular auction” → wrapper produces a String identifier
//class Q5ProcessFun(partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
//  private val logger = Logger("Q5ProcessFun")
//  Logger.setLevel("Q5ProcessFun", "INFO")
//  logger.info("Starting Q5ProcessFun")
//
//  override protected def processWindow(window: Long, states: List[Any]): String = {
//    val counts = states.head.asInstanceOf[Map[String, GCounter]]
//    AuctionGCounterWrapper.value(counts)
//  }
//}
