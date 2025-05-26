package holon

import holon.backend.HolonImpl

trait Holon {
  def submitOrUpdateJob(job: Job): Unit
  def stop(): Unit
  def partitions(): List[Int]
}

object Holon {
  def apply(nodeId: Int): Holon = HolonImpl(nodeId)
}
