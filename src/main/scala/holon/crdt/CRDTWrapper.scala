package holon.crdt

import org.apache.pekko.cluster.ddata.SelfUniqueAddress

trait CRDTWrapper[T] {
  def empty: T

  // Existing method for simple counters.
  def increment(crdt: T, address: SelfUniqueAddress, delta: Long): T

  // Overloaded method for map-based CRDTs,
  def increment(crdt: T, address: SelfUniqueAddress, delta: Long, key: String): T

  def merge(a: T, b: T): T

  def value(crdt: T): BigInt
  
  //TODO: find better way to implement this  
  def value(crdt: T, string: String): Map[String, BigInt]
}
