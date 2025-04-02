package holon.crdt

import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}

object GCounterWrapper extends CRDTWrapper[GCounter] {
  override def empty: GCounter = GCounter.empty

  override def increment(crdt: GCounter, address: SelfUniqueAddress, delta: Long): GCounter =
    crdt.increment(address, delta)

  override def merge(a: GCounter, b: GCounter): GCounter =
    a.merge(b)

  override def value(crdt: GCounter): BigInt =
    crdt.value
}

