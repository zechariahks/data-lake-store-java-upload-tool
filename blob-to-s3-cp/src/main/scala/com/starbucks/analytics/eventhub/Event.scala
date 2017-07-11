package com.starbucks.analytics.eventhub

import com.google.gson.Gson

class Event(uri: String, sharedAccessSignatureToken: String) {
  def toJson: String =
    s"""{
       |    "uri": $uri,
       |    "sharedAccessSignatureToken": $sharedAccessSignatureToken
       |}
     """.stripMargin

  def getUri = this.uri
  def getSASToken = this.sharedAccessSignatureToken
}
