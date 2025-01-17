package holon

import holon.backend.HolonImpl

trait Holon {
  def submitOrUpdate(job: Job): Unit
}

object Holon {
  def apply(): Holon = HolonImpl()
}
