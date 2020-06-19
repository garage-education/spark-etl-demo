package com.gability.scala.common.io

import com.gability.scala.common.io.FilesHandler._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

//TODO: Create unit testing
class HiveUtils(ss: SparkSession) {

  //TODO: write header documentation
  def parseCSVInputFile(filePath:      String,
                        fileDelimiter: String = "|",
                        hasHeader:     Boolean = true,
                        fileFormat:    String = "csv"): Dataset[Row] = {
    ss.read
      .format(fileFormat)
      .option("delimiter", fileDelimiter)
      .option("header", hasHeader)
      .load(filePath)
  }

  //TODO: write header documentation
  def createTmpHiveTable(filePath:      String,
                         tableName:     String,
                         fileDelimiter: String = "|",
                         hasHeader:     Boolean = true,
                         fileFormat:    String = "csv"): Unit = {
    parseCSVInputFile(filePath, fileDelimiter, hasHeader, fileFormat)
      .createOrReplaceTempView(tableName)
  }

//TODO: write header documentation
  def createTmpHiveTableWithDefaultName(fileName:      String,
                                        fileDelimiter: String = "|",
                                        hasHeader:     Boolean = true,
                                        fileFormat:    String = ".csv"): Unit = {
    parseCSVInputFile(getResourceTestPath(fileName + fileFormat), fileDelimiter, hasHeader)
      .createOrReplaceTempView(fileName.split("/").last)
  }

}