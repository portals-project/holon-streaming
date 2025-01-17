package holon.backend

import holon.*

class HolonImpl extends Holon {
  private val recovery = Recovery()

  def submitOrUpdate(job: Job): Unit = {
    recovery.submitOrUpdate(job)
  }
}
