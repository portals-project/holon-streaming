package holon.crdt

import org.apache.pekko.cluster.ddata.{SelfUniqueAddress, ReplicatedDelta}

trait CRDTWrapper[T, V] {
  type EventType

  def checkType(tsEvent: Any): Option[EventType]

  def timeStamp(event: EventType): Long

  def empty(address: SelfUniqueAddress): T

  def update(crdt: T, address: SelfUniqueAddress, delta: EventType): T

  def updateWithDelta(crdt: T, addr: SelfUniqueAddress, e: EventType): (T, Option[ReplicatedDelta])

  def merge(a: T, b: T): T

  def mergeDelta(crdt: T, delta: ReplicatedDelta): T

  def value(crdt: T): V
}
