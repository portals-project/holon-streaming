package holon.backend

import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import java.nio.file.{Files, Paths}
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.StorageOptions
import java.io.FileInputStream
import java.util.Base64


object GCSClient {

    val credentials = GoogleCredentials.fromStream(new FileInputStream("/Users/kolya/kth_projects/holon-streaming/.gcp/gcs-service-account.json"))
    val storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService

    val bucketName: String = "failure-recovery-dev"

    /**
     * Uploads string to Google Cloud Storage bucket
     * @param bucketName    bucket name
     * @param objectName    name of the string-object to upload
     * @param content       String content
     */
    def uploadStringToBucket(bucketName: String, objectName: String, content: String): Unit = {
        val blobId = BlobId.of(bucketName, objectName)
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        storage.create(blobInfo, content.getBytes("UTF-8"))
        println(s"String uploaded to gs://$bucketName/$objectName")
    }

    /**
     * Check if an object already exists in the bucket.
     * @param bucketName    bucket name
     * @param objectName    name of object to check
     * @return              Boolean
     */
    def checkIfFileExists(bucketName: String, objectName: String): Boolean = {
        storage.get(bucketName, objectName) != null
    }

    /**
     * Downloads string-object from GCS bucket. Does not check if object exists.
     * @param bucketName    bucket name
     * @param objectName    name of string-object to download
     * @return              downloaded string content.
     */
    def downloadStringFromBucket(bucketName: String, objectName: String): String = {
        val blob = storage.get(bucketName, objectName)
        new String(blob.getContent())
    }
}
