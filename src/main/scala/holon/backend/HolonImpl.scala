package holon.backend

import holon.*

class HolonImpl(nodeId: Int) extends Holon {
  private val recovery = Recovery(nodeId)

  def submitOrUpdate(job: Job): Unit = {
    recovery.submitOrUpdate(job)
  }

  def partitions(): List[Int] = {
    recovery.partitions()
  }

  def stop(): Unit = {
    recovery.stop()
  }
}
