package com.gability.scala.common.utils

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{input_file_name, udf}
import org.apache.spark.sql.types.StructType
import org.json4s.DefaultFormats

/** LogicUtils case class contains the utils function for ETL
  *
  * @param spark: SparkSession for spark api functions.
  */
case class LogicUtils(spark: SparkSession) extends Serializable with Logging {

  type metadataJson = Seq[Seq[String]]

  val getFileNameFromPathUDF: UserDefinedFunction =
    udf[String, String](_.split("/").last.split('.').head)

  /** readCsvWithFileNameAsColumn read input csv file using spark csv lib and cast it into Dataset[T] type case class
    *
    * @param T           : generic type case class passed to function for dataset casting
    * @param inputPath   : String input path for the csv file
    * @param inputSchema : StructType file schema and column names
    * @return Dataset[T] parsed dataset for the input file with additional column fileName contains the source file name
    */
  def readCsvWithFileNameAsDS[T](inputPath: String, inputSchema: StructType)(
    implicit encoder: Encoder[T]
  ): Dataset[T] = {
    spark.read
      .option("header", "true")
      .schema(inputSchema)
      .csv(inputPath)
      .withColumn("fileName", getFileNameFromPathUDF(input_file_name()))
      .as[T]
  }

  /**
    * get an instance of org.apche.spark.sql.SaveMode.{Append or Overwrite} matching string
    *
    * @param saveModeStr: String Has a value of Append or Overwrite,specified in param/config file of each spark job,indicating
    *                       whetehr to append or overwrite data into final target table.
    * @return SaveMode: an instance of org.apche.spark.sql.SaveMode.{Append or Overwrite} matching string
    */
  def getDataLoadStrategy(saveModeStr: String): SaveMode = {
    saveModeStr match {
      case append if (append.equalsIgnoreCase("append")) => SaveMode.Append
      case overwrite if (overwrite.equalsIgnoreCase("overwrite")) =>
        SaveMode.Overwrite
      case _ => throw new Exception("Unsupported SaveMode= " + saveModeStr)
    }
  }

}
