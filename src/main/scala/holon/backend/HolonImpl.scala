package holon.backend

import holon.*

class HolonImpl(number: Int) extends Holon {
  private val recovery = Recovery(number)

  def submitOrUpdate(job: Job): Unit = {
    recovery.submitOrUpdate(job)
  }
}
