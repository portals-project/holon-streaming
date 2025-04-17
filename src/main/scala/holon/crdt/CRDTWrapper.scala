package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.SelfUniqueAddress

trait CRDTWrapper[T] {
  def empty: T
  
  // Better name: Update, not every CRDT `increments`.
  def increment(crdt: T, address: SelfUniqueAddress, delta: Nexmark.Events.Bid): T

  def merge(a: T, b: T): T
  
  def value(crdt: T): String
}
