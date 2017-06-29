package com.starbucks.analytics.blob

import com.microsoft.azure.storage.{ CloudStorageAccount, OperationContext }
import com.microsoft.azure.storage.blob._
import com.typesafe.scalalogging.Logger

import scala.util.Try

/**
 * Manages all the Azure Blob Store operations
 */
object BlobManager {
  private val logger = Logger("BlobManager")

  /**
   * Loan a Azure Blob Store client
   *
   * @param connectionInfo Azure Data Blob Store Connection Information
   * @param f              Function that takes the Azure Data Lake Store
   *                       connection and returns the result of type R
   * @tparam R Type of the return value
   * @return Return value
   */
  private def withAzureBlobStoreClient[R](
    connectionInfo: BlobConnectionInfo,
    f:              (CloudBlobClient) => R
  ): Try[R] = {
    val storageConnectionString = s"DefaultEndpointsProtocol=https;" +
      s"AccountName=${connectionInfo.accountName};" +
      s"AccountKey=${connectionInfo.accountKey};" +
      s"EndpointSuffix = core.windows.net"

    val account = CloudStorageAccount.parse(storageConnectionString)
    val serviceClient = account.createCloudBlobClient()
    val result = Try(f(serviceClient))
    result
  }

  /**
   * Loan a Azure Blob Storage Container
   *
   * @param connectionInfo Azure Data Blob Store Connection Information
   * @param containerName Name of the container
   *
   * @return Reference to the cloud blob container
   */
  private def withAzureBlobContainer[R](
    connectionInfo: BlobConnectionInfo,
    containerName:  String,
    f:              (CloudBlobContainer) => R
  ): Try[R] = {
    def fn(serviceClient: CloudBlobClient): R = {
      val container = serviceClient.getContainerReference(containerName)
      val blobRequestOptions = new BlobRequestOptions()
      val operationContext = new OperationContext()
      blobRequestOptions.setConcurrentRequestCount(100)
      operationContext.setLoggingEnabled(true)
      container.createIfNotExists(
        BlobContainerPublicAccessType.OFF,
        blobRequestOptions,
        operationContext
      )
      f(container)
    }
    withAzureBlobStoreClient(
      connectionInfo,
      fn
    )
  }

  /**
   * Creates a reference to the block blob within
   * the specific container
   *
   * @param connectionInfo Azure Data Blob Store Connection Information
   * @param containerName Name of the container
   * @param blobName Name of the blob within the container
   * @return
   */
  def getBlockBlobReference(
    connectionInfo: BlobConnectionInfo,
    containerName:  String,
    blobName:       String
  ): Try[CloudBlockBlob] = {
    def fn(container: CloudBlobContainer): CloudBlockBlob = {
      container.getBlockBlobReference(blobName)
    }
    withAzureBlobContainer[CloudBlockBlob](
      connectionInfo,
      containerName,
      fn
    )
  }
}
