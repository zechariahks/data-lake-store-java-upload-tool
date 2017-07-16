package com.starbucks.analytics.s3

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.microsoft.azure.storage.blob.BlobInputStream
import com.typesafe.scalalogging.Logger

/**
  * S3 manager with all utility functions.
  */
object S3Manger {
  private val logger = Logger("S3Manager")

  /**
    * Method to upload azure blobInputStream to the s3 bucket.
    * @param data BlobInputStream of the Azure blob file.
    * @param bucketName AWS s3 Bucket name to transfer file to.
    * @param folderName AWS s3 Folder name to transfer file to.
    * @param blobName Azure blob file name.
    * @param blobSize Azure blob file-content size.
    * @param s3Client AWS s3 Client object with credentails.
    * @return
    */
  def uploadToS3(data: BlobInputStream, bucketName: String, folderName: String, blobName: String, blobSize: Long, s3Client: AmazonS3Client): Boolean ={
    try{
      logger.info(s"S3 bucket upload for file ${blobName} to bucket ${bucketName} for size ${blobSize}")
      val fileName = folderName+ "/" +blobName.split("\\/").last
      if(!checkIfExists(bucketName, fileName, blobSize, s3Client)) {
        val metadata = new ObjectMetadata()
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
        metadata.setContentLength(blobSize)
        s3Client.putObject(new PutObjectRequest(bucketName, fileName, data, metadata))
        true
      } else {
        logger.warn(s"File ${fileName} with same content length already exists in s3Bucket: ${bucketName}")
        false
      }
    } catch {
      case e: Exception => {
        logger.error(s"Uploading ${blobName} to s3Bucket: ${bucketName} failed.")
        logger.error(e.getCause.toString)
        false
      }
    }
  }

  /**
    * Method to check for the file in s3 with the same content length
    * @param bucketName AWS s3 Bucket name to check file into.
    * @param fileName AWS s3 key/file to check for.
    * @param fileSize AWS s3 file-content size to match.
    * @param s3Client AWS s3 Client with credentials.
    * @return Boolean representing whether the file exists or not.
    */
  def checkIfExists(bucketName: String, fileName: String, fileSize: Long, s3Client: AmazonS3Client): Boolean = {
    var doesExists: Boolean = false
    if(s3Client.doesObjectExist(bucketName, fileName)) {
      if(s3Client.getObjectMetadata(bucketName, fileName).getContentLength == fileSize) doesExists = true
    }
    doesExists
  }
}
