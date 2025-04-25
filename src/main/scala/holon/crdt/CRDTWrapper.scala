package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.SelfUniqueAddress

trait CRDTWrapper[T] {
  def empty(address: SelfUniqueAddress): T
  
  def increment(crdt: T, address: SelfUniqueAddress, delta: Nexmark.Events.TimeStampedEvent): T

  def merge(a: T, b: T): T
  
  def value(crdt: T): String
}
