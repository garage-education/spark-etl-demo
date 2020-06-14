package com.gability.scala

import com.gability.scala.common.io.HadoopFileHandler
import com.gability.scala.common.metadata.Metadata.{JobConfig, JobParamRawDtl}
import com.gability.scala.common.utils.EtlUtils._
import com.gability.scala.Metadata.ercsnStructSchema
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.lit

/** ETLPipelineLogic case class handle the ETL pipeline logic.
  *
  * @param jobConfig: JobConfig object for spark functionality.
  */
case class ETLPipelineLogic(jobConfig: JobConfig) extends Logging {
  val spark: SparkSession = jobConfig.sparkSession
  import spark.implicits._
  def jobLogicRunner(): Unit = {
    logger.info("Start Reading json from param file")
    val batchId: Long = jobConfig.configDS.map(_.batchId).head()
    val jsonStr: String = jobConfig.configDS.map(_.jobParams("json")).head()

    logger.info("parsing json string as JobParamRawDtl")
    val param: JobParamRawDtl = getInputFileParam[JobParamRawDtl](jsonStr)

    logger.info("read delimited file and compare with struct type")
    val (validDs, inValidDs) = HadoopFileHandler.readDelimitedFile(param, ercsnStructSchema, spark)

    logger.info("adding batchId to invalid source system")
    val invalidDsWithBatch = inValidDs.withColumn("batch_id", lit(batchId))

    logger.info("write rejected records")
    HadoopFileHandler.writeDelimitedFile(param.rejectedRecordsPath, invalidDsWithBatch)

    logger.info("Apply ETL rules")
//    /val dataContext =

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
