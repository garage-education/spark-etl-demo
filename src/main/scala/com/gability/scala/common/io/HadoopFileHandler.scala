package com.gability.scala.common.io

import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.EtlUtils._
import com.gability.scala.Metadata.InputRow
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{input_file_name, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

object HadoopFileHandler {

  def readDelimitedFile(param: JobParamRawDtl, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("delimiter", param.dataFileDelimiter)
      .option("header", param.header)
      .csv(param.inputSourcePath + param.processingSuffix)
      .withColumn("file_name", getFileNameFromPathUDF(input_file_name()))

  }

  def writeDelimitedFile(rejectionPath: String, rejectedDs: Dataset[Row]): Unit = {
    rejectedDs.write
      .json(rejectionPath)
  }

}
