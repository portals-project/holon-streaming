package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.SelfUniqueAddress

trait CRDTWrapper[T] {
  type EventType
  
  def checkType(tsEvent: Nexmark.Events.TimeStampedEvent): Option[EventType]
  
  def timeStamp(event: EventType): Long
  
  def empty(address: SelfUniqueAddress): T
  
  def increment(crdt: T, address: SelfUniqueAddress, delta: EventType): T

  def merge(a: T, b: T): T
  
  def value(crdt: T): String
}
