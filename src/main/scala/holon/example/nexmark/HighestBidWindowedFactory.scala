//package holon.example.nexmark
//
//import holon.{ProcFun, ProcFunFactory}
//import holon.backend.WindowedRecordProcFun
//import org.apache.pekko.cluster.ddata.{GCounter, LWWRegister}
//import holon.crdt.HighestBidLWWRegisterWrapper
//import holon.example.Nexmark.Events.Bid
//import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
//
//class HighestBidWindowedFactory extends ProcFunFactory {
//  override def create(partition: Int): ProcFun = {
//    implicit val wrapper: HighestBidLWWRegisterWrapper.type = HighestBidLWWRegisterWrapper
//
//    new WindowedRecordProcFun[LWWRegister[Array[Byte]], String](partition, wrapper, lwwRegisterBytesRW, ReadWriter[String])
//  }
//}