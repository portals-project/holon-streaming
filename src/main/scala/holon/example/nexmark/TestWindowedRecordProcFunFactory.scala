//package holon.example.nexmark
//
//import holon.{ProcFun, ProcFunFactory}
//import holon.backend.WindowedRecordProcFun
//import holon.crdt.{GCounterWrapper, TestCRDTWrapper}
//import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}
//import upickle.default.*
//
//// This factory creates WindowedRecordProcFun instances using GCounter as the CRDT
//class TestWindowedRecordProcFunFactory extends ProcFunFactory {
//  override def create(partition: Int): ProcFun = {
//    implicit val wrapper: TestCRDTWrapper.type = TestCRDTWrapper
//    
//    new WindowedRecordProcFun[Map[String, Long]](partition)
//  }
//}
