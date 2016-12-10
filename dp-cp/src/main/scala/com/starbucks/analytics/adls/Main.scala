package com.starbucks.analytics.adls

import java.io.{File, FileReader}

import com.starbucks.analytics.parsercombinator.{Compiler, UploaderLexer}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Database Connection String abstraction
 *
 * @param driver              Database driver
 * @param connectionStringUri Connection string Uri
 * @param username            Username
 * @param password            Password
 */
case class DBConnectionInfo(
  driver:              String,
  connectionStringUri: String,
  username:            String,
  password:            String
)

/**
 * Represents the Azure Data Lake Store connection information
 *
 * @param clientId                    Client Id of the application you registered with active directory
 * @param clientKey                   Client key of the application you registered with active directory
 * @param authenticationTokenEndpoint OAuth2 endpoint for the application
 *                                    you registered with active directory
 * @param accountFQDN                 Fully Qualified Domain Name of the Azure data lake store
 */
case class ADLSConnectionInfo(
  clientId:                    String,
  clientKey:                   String,
  authenticationTokenEndpoint: String,
  accountFQDN:                 String
)

/**
 * Represents the configuration required for the application
 * @param file Absolute path of the file containing upload instruction
 */
case class Config(file: File = new File("."))

/**
 * Entry point for the application
 * Orchestrator
 */
object Main extends App {
  val rootLogger: Logger = LoggerFactory.getLogger(
    org.slf4j.Logger.ROOT_LOGGER_NAME
  )

  // Parse the command line arguments
  // Exit if there is a problem parsing the command line arguments
  val config = parse(args)
  if (config.isEmpty) {
    System.exit(-1)
  }

  logStartupMessage(rootLogger, getApplicationName, config.get)

  // Lex
  val reader = new FileReader(config.get.file)
  //  val lexResult = UploaderLexer.parse(reader)
  //  rootLogger.debug(
  //    s"""Lexical result of parsing ${config.get.file.getAbsolutePath}:
  //       |\t\t $lexResult
  //     """.stripMargin
  //  )
  val ast = Compiler(reader)
  rootLogger.debug(
    s"""
       |AST generated by parsing ${config.get.file.getAbsolutePath}
       |\t\t $ast
     """.stripMargin
  )

  // Utility function to return the application name
  private def getApplicationName: String = new java.io.File(classOf[App]
    .getProtectionDomain
    .getCodeSource
    .getLocation
    .getPath)
    .getName

  /**
   * Parses the command line arguments using scopt
   *
   * @param args Command line arguments
   * @return A valid configuration object parsing was successful
   */
  def parse(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config](getApplicationName) {
      override def showUsageOnError = true

      // Setup the parser
      head(getApplicationName)
      help("help") text "prints this usage text"
      opt[File]('f', "fileName")
        .valueName("<file name>")
        .required()
        .validate(f =>
          if (f exists) success
          else failure(s"The file ${f.getAbsolutePath} should exist."))
        .action { (x, c) => c.copy(file = x) }
        .text("File containing the uploader")
    }

    // Evaluate
    parser.parse(args, Config()) match {
      case Some(config) =>
        Some(config)
      case None =>
        None
    }
  }

  /**
   * Logs a startup message to the log
   *
   * @param logger          Logger used by the application
   * @param applicationName Name of the application
   * @param config          Data Transfer configuration
   */
  private def logStartupMessage(
    logger:          Logger,
    applicationName: String,
    config:          Config
  ): Unit = {
    logger.info(s"$applicationName starting with command line arguments: ")
    logger.info(s"\t Filename: ${config.file.getAbsolutePath}")
  }
}