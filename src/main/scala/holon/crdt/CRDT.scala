package holon.crdt

import upickle.default.*

import org.apache.pekko.actor.*
import org.apache.pekko.cluster.*
import org.apache.pekko.cluster.ddata.*

object CRDT:
  private final val protocolName = "pekko"
  private final val systemName = "MyActorSystem"
  def address(id: Int) =
    SelfUniqueAddress(
      UniqueAddress(
        Address(protocolName, systemName, "", 0),
        id.toLong,
      )
    )

  private final val extendedActorSystem = org.apache.pekko.actor.ActorSystem(systemName)
  private final val serializer_rdata = new org.apache.pekko.cluster.ddata.protobuf.ReplicatedDataSerializer(extendedActorSystem.asInstanceOf)

  private def crdtToBinary(obj: AnyRef): Array[Byte] =
    writeBinary((serializer_rdata.toBinary(obj), serializer_rdata.manifest(obj)))

  private def crdtFromBinary(bytes: Array[Byte]): AnyRef =
    val (b, manifest) = readBinary[Tuple2[Array[Byte], String]](bytes)
    serializer_rdata.fromBinary(b, manifest)

  private def tagWithManifest(manifest: String, bytes: Array[Byte]): Array[Byte] =
    writeBinary((manifest, bytes))

  private def untagWithManifest(bytes: Array[Byte]): (String, Array[Byte]) =
    readBinary(bytes)

  def crdtToBinaryWithManifest(manifest: String, obj: AnyRef): Array[Byte] =
    tagWithManifest(manifest, crdtToBinary(obj))

  def crdtFromBinaryWithManifest(bytes: Array[Byte]): (String, AnyRef) =
    untagWithManifest(bytes) match
      case (manifest, bytes) =>
        (manifest, crdtFromBinary(bytes))
