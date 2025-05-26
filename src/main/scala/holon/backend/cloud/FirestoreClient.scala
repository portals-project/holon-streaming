package holon.backend.cloud

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.firestore.{DocumentReference, Firestore, FirestoreOptions}
import holon.*
import holon.Config.FIRESTORE_START_KEY

import java.io.FileInputStream
import scala.jdk.CollectionConverters.*

object FirestoreClient {

    val FLAG_COLLECTION = FIRESTORE_START_KEY

    val credentialsPath = sys.env.getOrElse("GOOGLE_FIRESTORE_CREDENTIALS", "/Users/kolya/kth_projects/holon-streaming/.gcp/firebase-user-account.json")
    val credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath))
    val firestore: Firestore = FirestoreOptions.newBuilder().setCredentials(credentials).build().getService
    val logger = Logger.apply("FirestoreClient")
    Logger.setLevel("FirestoreClient", "INFO")

    /**
     * Checks the `start_flag` document in the `FLAG_COLLECTION` to see if the `start` property is true or false.
     *
     * @return Boolean indicating the value of the `start` property.
     */
    def isStartFlagSet: Boolean = {
        val docRef: DocumentReference = firestore.collection(FLAG_COLLECTION).document("start_flag")
        val document = docRef.get().get()
        if (document.exists()) {
            document.getData.asScala.get("start").exists(_.asInstanceOf[Boolean])
        } else {
            false
        }
    }


    def main(args: Array[String]): Unit = {
        
        val start = isStartFlagSet
        println(start)

    }

}