package com.starbucks.analytics

import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import com.microsoft.azure.servicebus.ConnectionStringBuilder

/**
  * Created by depatel on 7/11/17.
  */
object EventHubSenderDemo {

  def main(args: Array[String]): Unit = {
    val namespaceName = "dpeventhubdemo"
    val eventHubName = "dpeventhubtest"
    val sasKeyName = "RootManageSharedAccessKey"
    val sasKey = "R2BOqSnxpO7siJLeKkmlNue8SzLfHsfJVLTSkv1/VnM="
    val connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey)

    val payloadEvent = "Test1111111 Event messages sent from the sample event hub publisher.".getBytes("UTF-8")
    val eventData = new EventData(payloadEvent)

    val eventhubClient = EventHubClient.createFromConnectionStringSync(connStr.toString)
    try {
      eventhubClient.sendSync(eventData)
    } catch {
      case e :Exception => println(e.getCause); e.printStackTrace()
    }
  }
  println("Hello")
  System.exit(0)
}
