//package holon.example.nexmark
//
//import holon.ProcFun
//import holon.ProcFunFactory
//import holon.backend.WindowedRecordProcFun
//import holon.crdt.AuctionGCounterWrapper
//import org.apache.pekko.cluster.ddata.GCounter
//
//// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
//class AuctionPopularityWindowedFactory extends ProcFunFactory {
//  override def create(partition: Int): ProcFun = {
//    // Use the new AuctionGCounterWrapper for mapping auctions to their bid counters.
//    implicit val wrapper: AuctionGCounterWrapper.type = AuctionGCounterWrapper
//
//    new WindowedRecordProcFun[Map[String, GCounter]](partition)(
//      crdt = wrapper,
//      rw = holon.serialization.mapGcounterRW
//    )
//  }
//}