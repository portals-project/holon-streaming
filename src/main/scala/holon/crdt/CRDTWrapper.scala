package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.SelfUniqueAddress

trait CRDTWrapper[T] {
  def empty: T
  
  // Existing method for simple counters.
  def increment(crdt: T, address: SelfUniqueAddress, delta: Nexmark.Events.Bid): T

  def merge(a: T, b: T): T
  
  //TODO: find better way to implement this  
  def value(crdt: T): String
}
