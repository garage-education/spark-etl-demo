package com.gability.scala.common.utils

import java.net.URL

import com.gability.scala.common.io.FilesHandler.checkFileExists
import org.apache.logging.log4j.scala.Logging

import scala.language.postfixOps
import scala.sys.process._

/** Factory for DataDownloader instances. */
object DataDownloader extends Logging {

  /** Download data from online link
    *
    * @param fileURL         :String represents the file url the  their name
    * @param destinationPath :String destination folder and file path
    */
  def downloadOnlineData(fileURL: String, destinationPath: String): Unit = {
    if (checkFileExists(destinationPath)) {
      logger.warn("file " + destinationPath + " already exists")
    } else {
      new URL(fileURL) #> new java.io.File(destinationPath) !!;
      logger.info("file " + destinationPath + " downloaded successfully")
    }
  }

}
