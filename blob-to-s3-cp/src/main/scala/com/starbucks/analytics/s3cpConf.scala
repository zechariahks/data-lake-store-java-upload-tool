package com.starbucks.analytics

import org.rogach.scallop.ScallopConf

/**
  * Created by depatel on 7/10/17.
  */
class s3cpConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner(
    s"""
       |Tool to copy securely from Azure Blob Store to Amazon s3 bucket
       |Example:
       |Java -jar $getApplicationName
       |  --spnClientId xx
       |  --spnClientKey xx
       |  --spnOATH2TokenEndPoint xx
       |  --blobStorageContainerName xx
       |  --blobStoreRootFolder xx
       |  --eventHubNamespaceName xx
       |  --eventHubName xx
       |  --eventHubSASKeyName xx
       |  --eventHubSASKey xx
     """.stripMargin
  )

  private def getApplicationName: String = new java.io.File(classOf[App]
    .getProtectionDomain
    .getCodeSource
    .getLocation
    .getPath)
    .getName
}
