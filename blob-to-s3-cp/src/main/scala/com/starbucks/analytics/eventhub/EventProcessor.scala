package com.starbucks.analytics.eventhub

import java.lang
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.google.gson.Gson
import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventprocessorhost.{CloseReason, IEventProcessor, PartitionContext}
import com.microsoft.azure.storage.OperationContext
import com.microsoft.azure.storage.blob.{BlobInputStream, BlobRequestOptions, CloudBlockBlob}
import com.starbucks.analytics.blob.BlobManager
import com.starbucks.analytics.s3.S3Manger
import com.typesafe.scalalogging.Logger
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}
import scala.util.{Failure, Success}

/**
  * Event Processor to process all the events received from Event Hub.
  */
class EventProcessor(awsAccessKeyId: String, awsSecretKey: String, s3BucketName: String, s3FolderName: String, desiredParallelism: Int) extends IEventProcessor{

  private val logger = Logger("EventProcessor")
  private var checkpointBatchingCount = 0
  private val aWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey)
  private val s3Client = new AmazonS3Client(aWSCredentials)

  def toEvent(eventString: String) = new Gson().fromJson(eventString, classOf[Event])

  /**
    * Method invoked at the time of event processor registration.
    * @param context
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  override def onOpen(context: PartitionContext): Unit = {
    logger.info(s"Partition ${context.getPartitionId} is opening.")
  }

  /**
    * Method invoked when event processor un-registration.
    * @param context
    * @param reason
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  override def onClose(context: PartitionContext, reason: CloseReason): Unit = {
    logger.info(s"Partition ${context.getPartitionId} is closing.")
  }

  /**
    * Method to invoke when error is thrown from event processor host.
    * @param context
    * @param error
    */
  override def onError(context: PartitionContext, error: Throwable): Unit = {
    logger.info(s"Partition ${context.getPartitionId} on Error: ${error.toString}")
  }

  /**
    * Method to ingest and process the events received from the event hub in batches.
    * @param context
    * @param messages
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  override def onEvents(context: PartitionContext, messages: lang.Iterable[EventData]): Unit = {

      logger.info(s"Partition ${context.getPartitionId} got message batch")
      var lastEventData: EventData = null
      val messagesList = messages.asScala
      var eventsList: ListBuffer[Event] = ListBuffer[Event]()
      for (message: EventData <- messagesList) {
        logger.info(s"(Partition: ${context.getPartitionId}, offset: ${message.getSystemProperties.getOffset}," +
          s"SeqNum: ${message.getSystemProperties.getSequenceNumber}) : ${new String(message.getBytes, "UTF-8")}")
        val msgString = new String(message.getBytes, "UTF-8")
        if (msgString.contains("uri") && msgString.contains("2017-07-17")) {
          val event: Event = toEvent(msgString)
          eventsList += event
        }
        lastEventData = message
      }
      val parallelListofEvents: ParSeq[Event] = eventsList.par
      parallelListofEvents.tasksupport = new ForkJoinTaskSupport(
        new scala.concurrent.forkjoin.ForkJoinPool(desiredParallelism)
      )
      parallelListofEvents.foreach(event => {
        logger.info(s"Start copying for file ${event.getUri}")
          val uris = event.getUri.split(";")
          val primaryUri = uris(0).split("=")(1).trim
          val secondaryUri = uris(1).split("=")(1).trim
          val sasUri = primaryUri.substring(1, primaryUri.length - 1) + "?" + event.getSASToken.trim
          logger.info("SAS URI for the blob is : " + sasUri)

          // Method to create and get Aure blob InputStream, blobName and blobSize.
          def getBlobStream(azureBlockBlob: CloudBlockBlob): (BlobInputStream, String, Long) = {
            val blobRequestOptions = new BlobRequestOptions()
            val operationContext = new OperationContext()
            blobRequestOptions.setConcurrentRequestCount(100)
            operationContext.setLoggingEnabled(true)
            azureBlockBlob.downloadAttributes()
            (azureBlockBlob.openInputStream(), azureBlockBlob.getName, azureBlockBlob.getProperties.getLength)
          }

          BlobManager.withSASUriBlobReference(sasUri, getBlobStream) match {
            case Failure(e) =>
              logger.error(s"Unable to get InputStream for ${sasUri}")
              logger.error(e.getCause.toString)
            case Success(blobStreamData) => {
              val blobInputStream = blobStreamData._1
              val blobName = blobStreamData._2
              val blobSize = blobStreamData._3
              val uploadResult = S3Manger.uploadToS3(blobInputStream, s3BucketName, s3FolderName, blobName, blobSize, s3Client)
              if (uploadResult)
                logger.info(s"${blobName} transfer to S3 ${s3BucketName} : SUCCESS")
              else
                logger.warn(s"${blobName} transfer to S3 ${s3BucketName} : FAILED.") //context.checkpoint(new EventData(event.toJson.getBytes()))
            }
          }
      })
      logger.info(s"Checkpointing last received event : ${lastEventData.toString}")
      context.checkpoint(lastEventData)
  }


}
