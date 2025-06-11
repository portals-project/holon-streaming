package holon.example.nexmark.queryfactories

import holon.example.nexmark.procfuns.Q0ProcessFun
import holon.{ProcFun, ProcFunFactory}

// This factory creates a Q0ProcessFun instance without any CRDTs
class Q0Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = { 
    new Q0ProcessFun(partition)
  }
}