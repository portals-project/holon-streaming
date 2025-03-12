package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.RecordProcFun

class RecordProcFunFactory extends ProcFunFactory {
    override def create(partition: Int): ProcFun = new RecordProcFun(partition)
}