package holon.backend

import holon.*

class HolonImpl(nodeId: Int) extends Holon {
  private val recovery = Control(nodeId)

  def submitOrUpdateJob(job: Job): Unit = {
    recovery.submitOrUpdateJob(job)
  }

  def partitions(): List[Int] = {
    recovery.partitions()
  }

  def stop(): Unit = {
    recovery.stop()
  }
}
