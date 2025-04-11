package holon.backend

import holon.*
import com.google.cloud.storage.{BlobId, BlobInfo}
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.StorageOptions
import java.io.FileInputStream
import Config.*


object GCSClient {

    val bucketName = sys.env.getOrElse("GCS_BUCKET_NAME", GCS_BUCKET_NAME)
    val credentialsPath = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", GC_CREDENTIALS_FILE_PATH)
    val credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath))
    val storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService
    val logger = Logger.apply("GCSClient")
    Logger.setLevel("GCSClient", "INFO")


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
        logger.debug(s"String uploaded to gs://$bucketName/$objectName")
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
