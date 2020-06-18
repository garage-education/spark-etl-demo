package com.gability.scala.common.io

import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.EtlUtils._
import com.gability.scala.Metadata.InputRow
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{input_file_name, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

object HadoopFileHandler {

  def readDelimitedFile(param: JobParamRawDtl, schemaStruct: StructType, spark: SparkSession): (Dataset[Row], Dataset[Row]) = {
    val inputDt = spark.read
      .option("delimiter", param.dataFileDelimiter)
      .option("header", param.header)
      .csv(param.inputSourcePath + param.processingSuffix)
      .withColumn("file_name", getFileNameFromPathUDF(input_file_name()))

    inputDt.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    //TODO: check to reduce the dataframe scan one idea is to add a new column with match Boolean flag
    val validDf = inputDt
    //.filter(schemaParser(_, schema2CaseClassValidator[InputRow]()))
      .filter(schemaParser(_, structSchemaValidator(schemaStruct)))

    //TODO: add handle to allow null fields rejection.
    //TODO: add rejection reason
    val inValidDf = inputDt
      .filter(!schemaParser(_, structSchemaValidator(schemaStruct)))

    (validDf, inValidDf)
  }

  def writeDelimitedFile(rejectionPath: String, rejectedDs: Dataset[Row]): Unit = {
    rejectedDs.write
      .json(rejectionPath)
  }

}
