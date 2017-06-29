package com.starbucks.analytics

import java.util

import com.microsoft.azure.datalake.store.ADLFileInputStream
import com.microsoft.azure.storage.blob.{CloudBlockBlob, SharedAccessBlobPermissions, SharedAccessBlobPolicy}
import com.starbucks.analytics.adls.{ADLSConnectionInfo, ADLSManager}
import com.starbucks.analytics.blob.{BlobConnectionInfo, BlobManager}
import com.starbucks.analytics.eventhub.{Event, EventHubConnectionInfo, EventHubManager}
import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParSeq
import scala.util.{Failure, Success, Try}

/**
 * Entry point for the application
 */
object Main {
  val logger = Logger("Main")
  var successMap = new mutable.LinkedHashMap[String, (Boolean, Option[String])]()

  /**
   * Entry point
   * @param args Command line arguments
   */
  def main(args: Array[String]) {
    val conf = new Conf(args)
    logger.info(conf.summary)

    // Setup ADLS Connection Information
    val adlsConnectionInfo = ADLSConnectionInfo(
      clientId = conf.spnClientId(),
      clientKey = conf.spnClientKey(),
      authenticationTokenEndpoint = conf.spnOauth2TokenEndpoint(),
      accountFQDN = conf.adlsFQDN()
    )
    // Setup BLOB Store Connection Information
    val blobConnectionInfo = BlobConnectionInfo(
      accountName = conf.blobStoreAccountName(),
      accountKey = conf.blobStoreAccountKey()
    )
    // Setup Event Hub Connection Information
    val eventHubConnectionInfo = EventHubConnectionInfo(
      eventHubNamespaceName = conf.eventHubNamespaceName(),
      eventHubName = conf.eventHubName(),
      sasKeyName = conf.eventHubSASKeyName(),
      sasKey = conf.eventHubSASKey()
    )

    // Get a list of files from the source
    val listOfFiles = getListOfFiles(
      conf,
      adlsConnectionInfo
    )
    if (listOfFiles.isEmpty) {
      logger.error("List of source files is empty. Quitting the application.")
      System.exit(-1)
    }

    // Setup parallelism
    val parallelListOfFiles: ParSeq[String] = listOfFiles.par
    parallelListOfFiles.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(conf.desiredParallelism())
    )

    // Start the parallel operation and collect the results
    parallelListOfFiles
      .foreach(file => {
        logger.info(s"Operating on source file $file" +
          s" in Azure Data Lake Store ${adlsConnectionInfo.accountFQDN}")
        val success: (Boolean, Option[String]) = upload(
          adlsConnectionInfo,
          blobConnectionInfo,
          conf,
          file
        )
        successMap += (file -> success)
      })

    // Validate the results
    if (successMap.exists((x) => !x._2._1)) {
      logger.error(s"Failed uploading the following files:" +
        s" ${successMap.filter((x) => !x._2._1).mkString("\n")}")
      System.exit(-1)
    } else {
      logger.info(s"Succeeded uploading the files: " +
        s" ${successMap.keys.mkString("\n")}")

      // Publish the events to the event hub
      val eventsToPublish = successMap.map(x =>
        new Event(
          fileName = x._1,
          sharedAccessSignatureToken = x._2._2.get
        )).toList
      EventHubManager.publishEvents(
        eventHubConnectionInfo,
        eventsToPublish
      )
    }
  }

  /**
   * Uploads the file from Azure Data Lake Store to Azure Blob Storage
   *
   * @param adlsConnectionInfo Azure Data Lake Store Connection Information
   * @param blobConnectionInfo Azure Data Blob Store Connection Information
   * @param conf Tool configuration
   * @param sourceFile File to upload
   * @return
   */
  def upload(
    adlsConnectionInfo: ADLSConnectionInfo,
    blobConnectionInfo: BlobConnectionInfo,
    conf:               Conf,
    sourceFile:         String
  ): (Boolean, Option[String]) = {
    var success = false
    var token: Option[String] = None
    BlobManager.getBlockBlobReference(
      blobConnectionInfo,
      conf.blobStoreContainerName(),
      s"${conf.blobStoreRootFolder.getOrElse("")}$sourceFile"
    ) match {
        case Failure(exception) => {
          logger.error(s"Error creating block blob $sourceFile" +
            s" in container ${conf.blobStoreContainerName}" +
            s" in storage account ${blobConnectionInfo.accountName}." +
            s" Exception: $exception")
          success = false
        }
        case Success(blockBlobReference: CloudBlockBlob) => {
            def fn(stream: ADLFileInputStream) = {
              var data = Array.fill[Byte](conf.desiredBufferSize() * 1000000)(0)
              while (stream.read(data) != -1) {
                blockBlobReference.uploadFromByteArray(
                  data,
                  0,
                  data.length
                )
              }
            }
          ADLSManager.withAzureDataLakeStoreFileStream[Boolean](
            adlsConnectionInfo,
            sourceFile,
            fn
          ) match {
            case Failure(exception) =>
              logger.error(s"Error uploading to block blob $sourceFile" +
                s" in container ${conf.blobStoreContainerName}" +
                s" in storage account ${blobConnectionInfo.accountName}." +
                s" Exception: $exception")
              success = false
            case Success(value) =>
              if (value) {
                logger.info(s"Trying to generate shared access signature for block blob" +
                  s" $sourceFile in containers ${conf.blobStoreContainerName}")
                Try(BlobManager.getSharedAccessSignatureToken(
                  blockBlobReference,
                  conf.tokenExpiration()
                )) match {
                  case Failure(exception) =>
                    logger.error(s"Error getting a shared access signature token" +
                      s" for block blob $sourceFile in container" +
                      s" ${conf.blobStoreContainerName} in storage account " +
                      s" ${blobConnectionInfo.accountName}. Exception: $exception")
                    success = false
                  case Success(signatureToken) =>
                    token = Some(signatureToken)
                    success = true
                }
              } else {
                success = false
              }
          }
        }
      }

    (success, token)
  }

  /**
   * Return a list of files from the source
   *
   * @param conf Configuration
   * @param adlsConnectionInfo Connection Information for
   *                           Azure Data Lake Store
   */
  def getListOfFiles(
    conf:               Conf,
    adlsConnectionInfo: ADLSConnectionInfo
  ): mutable.ListBuffer[String] = {
    // Set up the source structure that this tool needs to
    // operate on
    var listOfFiles = new mutable.ListBuffer[String]()
    val sourceFolder: String = conf.sourceFolder.getOrElse("/")
    if (conf.sourceFile.isSupplied) {
      listOfFiles += s"$sourceFolder${conf.sourceFile()}"
    } else {
      // Based on configuration setup, we should have got
      // a folder if the file is not specified
      ADLSManager.getFilePathsInFolder(
        adlsConnectionInfo,
        sourceFolder
      ) match {
        case Success(lf) =>
          lf.foreach(s => listOfFiles += s"$sourceFolder$s")
        case Failure(f) =>
          logger.error(s"Getting list of files from $sourceFolder" +
            s" in Azure Data Lake Store ${adlsConnectionInfo.accountFQDN}" +
            s" failed with exception $f")
      }
    }
    listOfFiles
  }
}
