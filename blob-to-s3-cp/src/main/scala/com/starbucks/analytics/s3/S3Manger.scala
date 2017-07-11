package com.starbucks.analytics.s3

import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}

/**
  * Created by depatel on 7/10/17.
  */
object S3Manger {

  //Creating a folder in the s3 bucket to upload.
  //TODO: Go to the specific folder given in the configutation.
  def createS3Folder(s3BucketName: String, s3FolderName: String, s3Client: AmazonS3Client): Unit ={
    // create metadata for the folder and its content-length to 0 default.
    val metadata = new ObjectMetadata()
    metadata.setContentLength(0)

    // create empty content for the folder.
    val emptyContent = new ByteArrayInputStream(new Array[Byte](0))

    // put request for the empty content in the folder.
    val putObjectRequest = new PutObjectRequest(s3BucketName, s3FolderName+"/", emptyContent, metadata)

    // send the put request.
    s3Client.putObject(putObjectRequest)
  }

}
