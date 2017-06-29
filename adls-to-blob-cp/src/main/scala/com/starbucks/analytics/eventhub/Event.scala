package com.starbucks.analytics.eventhub

class Event(fileName: String, sharedAccessSignatureToken: String) {
  def toJson: String =
    s"""{
       |    fileName: $fileName,
       |    sharedAccessSignatureToken: $sharedAccessSignatureToken
       |}
     """.stripMargin
}
