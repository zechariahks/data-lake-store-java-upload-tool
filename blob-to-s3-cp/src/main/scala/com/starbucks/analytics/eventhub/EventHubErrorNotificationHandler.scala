package com.starbucks.analytics.eventhub

import java.util.function.Consumer

import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs
import com.starbucks.analytics.eventhub.EventHubManager.logger
import com.typesafe.scalalogging.Logger

/**
  * Created by depatel on 7/10/17.
  */
class EventHubErrorNotificationHandler extends Consumer[ExceptionReceivedEventArgs] {
  private val logger = Logger("EventHubProcessorErrorHandler")

  override def accept(t: ExceptionReceivedEventArgs): Unit = {
    logger.error("Host "+ t.getHostname + " received general error notification during "+ t.getAction+ ": "+ t.getException.toString)
  }
}
