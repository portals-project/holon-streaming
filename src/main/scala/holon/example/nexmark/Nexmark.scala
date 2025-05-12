package holon.example

import upickle.default.*

import org.apache.beam.sdk.nexmark.*
import org.apache.beam.sdk.nexmark.model.*
import org.apache.beam.sdk.nexmark.sources.generator.*
import org.apache.beam.sdk.nexmark.sources.generator.Generator.*
import org.apache.beam.sdk.values.*

object Nexmark:
  final val AUCTIONS_MANIFEST = "A"
  final val BIDS_MANIFEST = "B"
  final val SNAPSHOT_MANIFEST = "C"
  final val PERSONS_MANIFEST = "D"

  private val reader = summon[ReadWriter[Events.TimeStampedEvent]]
  private val writer = summon[ReadWriter[Events.TimeStampedEvent]]
  inline def serialize(e: Events.TimeStampedEvent): Array[Byte] = writeBinary(e)(using writer)
  inline def deserialize(bytes: Array[Byte]): Events.TimeStampedEvent = readBinary(bytes)(using reader)

  def iterator(): Iterator[Events.TimeStampedEvent] =
    val nexmarkConfig = NexmarkConfiguration.DEFAULT

    nexmarkConfig.hotAuctionRatio = 2
    nexmarkConfig.hotBiddersRatio = 4
    nexmarkConfig.hotSellersRatio = 4

    val config = new GeneratorConfig(nexmarkConfig, 0, 0, 0, 0)
    val generator = new Generator(config)
    new Iterator[Events.TimeStampedEvent]:
      override def hasNext: Boolean = true
      override def next(): Events.TimeStampedEvent = toScala(generator.next())

  object Events:
    case class TimeStampedEvent(
        event: Event,
        timestamp: Long
    ) derives ReadWriter

    sealed trait Event derives ReadWriter

    case class Bid(
        auction: Long,
        bidder: Long,
        price: Long,
        dateTime: Long,
        extra: String
    ) extends Event
        derives ReadWriter

    case class Person(
        id: Long,
        name: String,
        emailAddress: String,
        creditCard: String,
        city: String,
        state: String,
        dateTime: Long,
        extra: String
    ) extends Event
        derives ReadWriter

    case class Auction(
        id: Long,
        itemName: String,
        description: String,
        initialBid: Long,
        reserve: Long,
        dateTime: Long,
        expires: Long,
        seller: Long,
        category: Long,
        extra: String
    ) extends Event
        derives ReadWriter

  private def toScala(e: TimestampedValue[Event]): Events.TimeStampedEvent =
    Events.TimeStampedEvent(
      toScala(e.getValue()),
      e.getTimestamp().getMillis()
    )

  private def toScala(e: Event): Events.Event =
    e match
      case e: Event if e.bid != null =>
        Events.Bid(
          e.bid.auction,
          e.bid.bidder,
          e.bid.price,
          e.bid.dateTime.getMillis(),
          e.bid.extra
        )
      case e: Event if e.newAuction != null =>
        Events.Auction(
          e.newAuction.id,
          e.newAuction.itemName,
          e.newAuction.description,
          e.newAuction.initialBid,
          e.newAuction.reserve,
          e.newAuction.dateTime.getMillis(),
          e.newAuction.expires.getMillis(),
          e.newAuction.seller,
          e.newAuction.category,
          e.newAuction.extra
        )
      case e: Event if e.newPerson != null =>
        Events.Person(
          e.newPerson.id,
          e.newPerson.name,
          e.newPerson.emailAddress,
          e.newPerson.creditCard,
          e.newPerson.city,
          e.newPerson.state,
          e.newPerson.dateTime.getMillis(),
          e.newPerson.extra
        )
