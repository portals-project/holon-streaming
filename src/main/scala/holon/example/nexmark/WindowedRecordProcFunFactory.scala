package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.WindowedRecordProcFun

class WindowedRecordProcFunFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = new WindowedRecordProcFun(partition)
}