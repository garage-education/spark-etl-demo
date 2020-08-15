package com.gability.scala.common.io

import com.gability.scala.common.metadata.Metadata.{InputSource, JobParamRawDtl}
import com.gability.scala.common.utils.EtlUtils._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{input_file_name, udf}

object HadoopFileHandler {
  val getFileNameFromPathUDF: UserDefinedFunction = udf[String, String](_.split("/").last.split('.').head)

  def readDelimitedFile(param: InputSource, spark: SparkSession): Dataset[Row] = {
    val inputColsNames: Seq[String] = param.inputSchema.map(_.columnName)
    spark.read
      .option("delimiter", param.dataFileDelimiter)
      .option("header", param.header)
      .csv(param.inputSourcePath + param.processingSuffix)
      .withColumn("file_name", getFileNameFromPathUDF(input_file_name()))
      .toDF(inputColsNames: _*)
  }

  //TODO: write header documentation
  def parseCSVInputFile(spark:         SparkSession,
                        filePath:      String,
                        fileDelimiter: String = "|",
                        hasHeader:     Boolean = true,
                        fileFormat:    String = "csv"): Dataset[Row] = {
    spark.read
      .format(fileFormat)
      .option("delimiter", fileDelimiter)
      .option("header", hasHeader)
      .option("quote", "\'") //TODO: explain this
      .load(filePath)
  }

  def writeDelimitedFile(rejectionPath: String, rejectedDs: Dataset[Row], dataFileDelimiter: String): Unit = {
    rejectedDs.write
      .option("delimiter", dataFileDelimiter)
      .csv(rejectionPath)
  }

}
