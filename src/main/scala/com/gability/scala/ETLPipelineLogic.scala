package com.gability.scala

import com.gability.scala.common.config.ETLConfigManagement
import com.gability.scala.common.io.HadoopFileHandler
import com.gability.scala.common.metadata.Metadata.{JobConfig, JobParamRawDtl}
import com.gability.scala.common.utils.EtlUtils
import com.gability.scala.common.utils.EtlUtils._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/** ETLPipelineLogic case class handle the ETL pipeline logic.
  *
  * @param jobConfig: JobConfig object for spark functionality.
  */
case class ETLPipelineLogic(jobConfig: JobConfig) extends Logging {
  val spark: SparkSession = jobConfig.sparkSession
  import spark.implicits._
  def jobLogicRunner(): Unit = {
    val jsonStr: String = jobConfig.configDS.map(_.jobParams("json")).head()
    val param: JobParamRawDtl = getInputFileParam[JobParamRawDtl](jsonStr)
    val (validDs, inValidDs) = HadoopFileHandler.readDelimitedFile(param, schemaStruct = null, spark) //ercsnSchemaStruct
    /*logger.info("Start Reading and Parsing input data sources")
    val dataContext = getInputDataContext(spark)*/

    /*logger.info("Start transformation for input data sources")
    val transformedData: DataFrame = getInputDataTransformed(spark, dataContext)*/

    /*logger.info("Write tranformed data to output path with repartition by option")
    saveOutputToPath(transformedData, srcPaths(4), "fileName")
    logger.info("Writing done.. ")*/
  }

  /*  private def getInputDataContext(sparkSession: SparkSession, srcPaths:Array[String]): DataContext = {
    InputDataCollector(sparkSession).getInputDataSourcesContext(srcPaths)
  }

  private def getInputDataTransformed(sparkSession: SparkSession, dataContext: DataContext): DataFrame = {
    DataTransformer(sparkSession, dataContext).prepareData()
  }

  private def saveOutputToPath(df: DataFrame, outPath: String, partitionCol: String): Unit = {
    df.write
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") //ignore _Success dummy files in hadoop/spark output
      .mode("overwrite")
      .format("json")
      .partitionBy(partitionCol)
      .save(outPath)
  }*/

}
