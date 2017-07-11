package com.starbucks.analytics

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit
import javax.sound.midi.Receiver

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.microsoft.azure.eventhubs.{EventHubClient, PartitionReceiver}
import com.microsoft.azure.eventprocessorhost.{EventProcessorHost, EventProcessorOptions}
import com.microsoft.azure.servicebus.ConnectionStringBuilder
import com.starbucks.analytics.blob.BlobConnectionInfo
import com.starbucks.analytics.eventhub.{EventHubConnectionInfo, EventHubErrorNotificationHandler, EventHubManager, EventProcessor}
import com.starbucks.analytics.s3.S3ConnectionInfo

import scala.concurrent.ExecutionException
import scala.util.Try


/**
  * Created by depatel on 7/10/17.
  */
object Main {

  def main(args: Array[String]): Unit = {

    //Setup Azure EventHub Connection
    val eventHubConnectionInfo = EventHubConnectionInfo(
      eventHubNamespaceName = "",
      eventHubName = "",
      sasKeyName = "",
      sasKey = ""
    )

    //Create EventHubProcessorHost to process the eventdata from event hub.
    val eventHubNamespaceName: String = "dpeventhubdemo"
    val eventHubName: String = "dpeventhubtest"
    val sasKeyName: String = "dpeventhubsas"
    val sasKey: String = "hcVwTZRAZKrW1GioqCqskcuQg21lam+rS9XV0Ud1q30="
    val storageAccountName: String = "dpeventhubdemostorage"
    val storageAccountKey: String = "9Vliyoma1P6GsIYf4q3BwLW3vwLoFor5qPHuQJsZPIGbigWMroiY+a6aG7b6Q4PyBefnBXsl9l16inGtHZAMFA=="
    //val storageConnectionString: String = "DefaultEndpointsProtocol=https;AccountName=" + storageAccountName + ";AccountKey=" + storageAccountKey
    val storageConnectionString: String = "DefaultEndpointsProtocol=https;AccountName=dpeventhubdemostorage;AccountKey=9Vliyoma1P6GsIYf4q3BwLW3vwLoFor5qPHuQJsZPIGbigWMroiY+a6aG7b6Q4PyBefnBXsl9l16inGtHZAMFA==;EndpointSuffix=core.windows.net"
    val eventHubConnectionString = new ConnectionStringBuilder(eventHubNamespaceName, eventHubName, sasKeyName, sasKey)
    val host = new EventProcessorHost("simpleeventhubproc",eventHubName, "$Default",eventHubConnectionString.toString, storageConnectionString, "depateleventhubprocstorage")
    println(s"Registering host named ${host.getHostName}")

    val options = new EventProcessorOptions
    options.setExceptionNotification(new EventHubErrorNotificationHandler)
    options.getReceiveTimeOut
    try{
      println("before")
      host.registerEventProcessor(classOf[EventProcessor], options).get()
      println("after")
    }
    catch {
      case e: ExecutionException => {
        println(e.getCause.toString)
      }
      case e: Exception => {
        println(e.toString)
      }
    }

    println("Press any key to stop: ")
    try{
      System.in.read()
      host.unregisterEventProcessor()
      println("Calling forceExecutorShutdown")
      EventProcessorHost.forceExecutorShutdown(120)
    }catch {
      case e: Exception => {
        println(e.toString)
        e.printStackTrace()
      }
    }
    //Setup Azure Blob Store Connection
//    val blobStoreConnectionInfo = BlobConnectionInfo(
//      accountName = "",
//      accountKey = ""
//    )
//
//    //Setup s3 Connection
//    val s3ConnectionInfo = S3ConnectionInfo(
//      awsAccessKey = "",
//      awsSecretAccessKey = ""
//    )
//
//    //Setup AWS Credentials
//    val awsCredentials: AWSCredentials = new BasicAWSCredentials(s3ConnectionInfo.awsAccessKey, s3ConnectionInfo.awsSecretAccessKey)
//
//    //Create AWS S3 Client
//    val s3Client: AmazonS3 = new AmazonS3Client(awsCredentials)
//    //Create a bucket or get list of the available bucket.
//    //Currently just create a bucket.
//    val s3BucketName = "sample-blob-upload-test"
//    s3Client.createBucket(s3BucketName)
//


  }
}
