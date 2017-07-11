package com.starbucks.analytics.eventhub

import java.lang

import scala.collection.JavaConverters._
import com.google.gson.Gson
import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventprocessorhost.{CloseReason, IEventProcessor, PartitionContext}
import com.microsoft.azure.storage.blob.BlobInputStream
import com.starbucks.analytics.blob.BlobManager
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}

/**
  * Created by depatel on 7/10/17.
  */
class EventProcessor extends IEventProcessor{

  private val logger = Logger("EventProcessor")

  private var checkpointBatchingCount = 0

  println("Now creating the eventhub processor host instance.")
  def toEvent(eventString: String) = new Gson().fromJson(eventString, classOf[Event])

  @throws(classOf[Exception])
  override def onOpen(context: PartitionContext): Unit = {
    println(s"Partition ${context.getPartitionId} is opening.")
  }

  @throws(classOf[Exception])
  override def onClose(context: PartitionContext, reason: CloseReason): Unit = {
    println(s"Partition ${context.getPartitionId} is closing.")
  }

  override def onError(context: PartitionContext, error: Throwable): Unit = {
    println(s"Partition ${context.getPartitionId} on Error: ${error.toString}")
  }

  @throws(classOf[Exception])
  override def onEvents(context: PartitionContext, messages: lang.Iterable[EventData]): Unit = {

    System.out.println("SAMPLE: Partition " + context.getPartitionId + " got message batch")
    var messageCount = 0
    import scala.collection.JavaConversions._
    for (data <- messages) {
      System.out.println("SAMPLE (" + context.getPartitionId + "," + data.getSystemProperties.getOffset + "," + data.getSystemProperties.getSequenceNumber + "): " + new String(data.getBody, "UTF8"))
      messageCount += 1
      this.checkpointBatchingCount += 1
      if ((checkpointBatchingCount % 5) == 0) {
        System.out.println("SAMPLE: Partition " + context.getPartitionId + " checkpointing at " + data.getSystemProperties.getOffset + "," + data.getSystemProperties.getSequenceNumber)
        context.checkpoint(data)
      }
    }
    System.out.println("SAMPLE: Partition " + context.getPartitionId + " batch size was " + messageCount + " for host " + context.getOwner)
//    println(s"Partition ${context.getPartitionId} got message batch")
//    var messageCount: Int = 0
//    val messagesList = messages.asScala
//    var eventsList: ListBuffer[Event] = ListBuffer[Event]()
//      for( message: EventData <- messagesList) {
//      println(s"(Partition: ${context.getPartitionId}, offset: ${message.getSystemProperties.getOffset}," +
//        s"SeqNum: ${message.getSystemProperties.getSequenceNumber}) : ${new String(message.getBytes, "UTF-8")}")
////      val event: Event = toEvent(new String(message.getBytes))
////      eventsList += event
//    }

//    val parallelListofEvents : ParSeq[Event] = eventsList.par
//    // set parallelism....Make it configurable.
//    parallelListofEvents.tasksupport= new ForkJoinTaskSupport(
//      new scala.concurrent.forkjoin.ForkJoinPool(5)
//    )

    // TODO : for each of the event object read the file from the blob store and upload to s3.

//    eventsList.foreach( event => {
//      logger.info(s"Start copying for file ${event.getUri}")
//      val sasUri = event.getUri + event.getSASToken
//      var data = Array.fill[Byte](5 * 1000000)(0)
//      println("Event received now going to read the blob related to it.")
//      val blobInputStream = BlobManager.readBlob(sasUri)
//      while(blobInputStream. != -1){
//        println("Read byte array of : "+data.length)
//        println("Read byte array : "+ data.toString)
//      }
//    })
  }


}
