package com.gability.scala.common.io

import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.EtlUtils._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.input_file_name

object HadoopFileHandler {

  def readDelimitedFile(param: JobParamRawDtl, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("delimiter", param.dataFileDelimiter)
      .option("header", param.header)
      .csv(param.inputSourcePath + param.processingSuffix)
      .withColumn("fileName", getFileNameFromPathUDF(input_file_name()))
  }

  def writeDelimitedFile(rejectionPath: String, rejectedDs: Dataset[Row], dataFileDelimiter: String): Unit = {
    rejectedDs.write
      .option("delimiter", dataFileDelimiter)
      .csv(rejectionPath)
  }

}
