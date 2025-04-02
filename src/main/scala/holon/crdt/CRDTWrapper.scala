package holon.crdt

import org.apache.pekko.cluster.ddata.SelfUniqueAddress

trait CRDTWrapper[T] {
  def empty: T

  def increment(crdt: T, address: SelfUniqueAddress, delta: Long): T

  def merge(a: T, b: T): T

  def value(crdt: T): BigInt
}
