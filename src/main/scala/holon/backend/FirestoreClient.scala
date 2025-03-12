package holon.backend

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.firestore.{DocumentReference, Firestore, FirestoreOptions}
import holon.*

import java.io.FileInputStream
import scala.jdk.CollectionConverters.*

object FirestoreClient {
    
    val OWNERSHIP_COLLECTION_NAME = "ownership_registry"

    val credentialsPath = sys.env.getOrElse("GOOGLE_FIRESTORE_CREDENTIALS", "/Users/kolya/kth_projects/holon-streaming/.gcp/firebase-user-account.json")
    val credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath))
    val firestore: Firestore = FirestoreOptions.newBuilder().setCredentials(credentials).build().getService
    val logger = Logger.apply("FirestoreClient")
    Logger.setLevel("FirestoreClient", "INFO")

    /**
     * Queries Firestore for partitions owned by a node.
     * @return List of tuples (partitionId, versionNr)
     */
    def queryPartitionsByNodeId(collectionName: String, nodeId: Int): List[(Int, Int)] = {
        val query = firestore.collection(collectionName).whereEqualTo("node_id", nodeId)
        val querySnapshot = query.get().get()
        val documents = querySnapshot.getDocuments.asScala
        documents.flatMap { doc =>
            for {
                partitionId <- doc.getData.asScala.toMap.get("partition_id").map(_.asInstanceOf[Long].toInt)
                versionNr <- doc.getData.asScala.toMap.get("version_nr").map(_.asInstanceOf[Long].toInt)
            } yield (partitionId, versionNr)
        }.toList
    }

    /**
     * Queries Firestore for the node that owns a partition.
     *
     * @return tuple of (node_id, version_nr) or (-1, -1) if not found
     */
    def queryNodeForPartition(collectionName: String, partitionId: Int): (Int, Int) = {
        val query = firestore.collection(collectionName).whereEqualTo("partition_id", partitionId)
        val querySnapshot = query.get().get()
        val documents = querySnapshot.getDocuments.asScala
        documents.flatMap { doc =>
            for {
                nodeId <- doc.getData.asScala.toMap.get("node_id").map(_.asInstanceOf[Long].toInt)
                versionNr <- doc.getData.asScala.toMap.get("version_nr").map(_.asInstanceOf[Long].toInt)
            } yield (nodeId, versionNr)
        }.headOption.getOrElse((-1, -1))
    }

    def setPartitionOwnership(partitionId: Int, nodeId: Int, versionNr: Int): Unit = {
        val docRef: DocumentReference = firestore.collection(OWNERSHIP_COLLECTION_NAME).document(partitionId.toString)
        val data = Map("node_id" -> nodeId, "partition_id" -> partitionId, "version_nr" -> versionNr)
        val javaMap = data.asJava
        val apiFuture = docRef.set(javaMap)
        apiFuture.get() // Wait for the write to complete
        logger.info(s"Partition ownership set for partition: $partitionId, node: $nodeId, version: $versionNr")
    }

    /**
     * Writes data to a Firestore collection.
     *
     * @param collectionName name of the collection
     * @param documentId     ID of the document
     * @param data           data to write (as a Map)
     */
    def writeData(collectionName: String, documentId: String, data: Map[String, Any]): Unit = {
        val docRef: DocumentReference = firestore.collection(collectionName).document(documentId)
        val javaMap = data.asJava
        val apiFuture = docRef.set(javaMap)
        apiFuture.get() // Wait for the write to complete
        logger.info(s"Data written to Firestore collection: $collectionName, document: $documentId")
    }

    def main(args: Array[String]): Unit = {
        //            val collectionName = "ownership_registry"
        //            val documentId = "test"
        //            val data = Map("node_id" -> 2, "partition_id" -> 3)
        //            writeData(collectionName, documentId, data)

        val listOfPartitions = queryPartitionsByNodeId("ownership_registry", 1)
        println(listOfPartitions)
        val node = queryNodeForPartition("ownership_registry", 3)
        println(node)

    }

}