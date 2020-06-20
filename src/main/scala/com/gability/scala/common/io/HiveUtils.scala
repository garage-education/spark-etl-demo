package com.gability.scala.common.io

import com.gability.scala.common.io.FilesHandler._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import HadoopFileHandler.parseCSVInputFile

class HiveUtils(ss: SparkSession) {

//TODO: write header documentation
  def createTmpHiveTableWithDefaultName(fileName: String, fileDelimiter: String = "|", hasHeader: Boolean = true, fileFormat: String = ".csv"): Unit = {
    parseCSVInputFile(ss, getResourceTestPath(fileName + fileFormat), fileDelimiter, hasHeader)
      .createOrReplaceTempView(fileName.split("/").last)
  }

}
