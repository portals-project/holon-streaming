package holon.crdt

import org.apache.pekko.cluster.ddata.SelfUniqueAddress

object TestCRDTWrapper extends CRDTWrapper[Map[String, Long]] {
  override def empty: Map[String, Long] = Map.empty

  override def increment(crdt: Map[String, Long], address: SelfUniqueAddress, delta: Long): Map[String, Long] =
    Map[String, Long](address.toString -> (crdt.getOrElse(address.toString, 0L) + delta))

  override def merge(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] =
    a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0L)) }

  override def value(crdt: Map[String, Long]): BigInt =
    BigInt(crdt.values.sum)
}

