package com.gability.scala

import com.gability.scala.common.io.HadoopFileHandler
import com.gability.scala.common.metadata.Metadata.{JobConfig, JobParamRawDtl}
import com.gability.scala.common.utils.EtlUtils._
import com.gability.scala.Metadata.{Conf, ErcsvInputData, ercsnStructSchema}
import com.gability.scala.common.utils.EtlUtils
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel

/** ETLPipelineLogic case class handle the ETL pipeline logic.
  *
  * @param jobConfig: JobConfig object for spark functionality.
  */
case class ETLPipelineLogic(jobConfig: JobConfig, props: Conf) extends Logging {

  val spark: SparkSession = jobConfig.sparkSession
  def jobLogicRunner(): (Dataset[ErcsvInputData], Dataset[Row], JobParamRawDtl) = {
    logger.info("Start Reading json from param file")
    val (jsonStr, batchId) = (jobConfig.configDS.jobParams("json"), jobConfig.configDS.batchId) //handle error

    logger.info("parsing json string as JobParamRawDtl")
    val param: JobParamRawDtl = getInputFileParam[JobParamRawDtl](jsonStr)

    logger.info("read delimited file and compare with struct type")
    val inputDs = HadoopFileHandler.readDelimitedFile(param.inputSource, spark)
    inputDs.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val (validDs, inValidDs) = EtlUtils.validateDataset(inputDs, ercsnStructSchema)

    logger.info("adding batchId to invalid source system")
    val invalidDsWithBatch: DataFrame = inValidDs.withColumn("batch_id", lit(batchId))
    logger.debug("%s".format(invalidDsWithBatch.show(truncate = false)))

    logger.info("get hive input data context")
    val inputDataContext = HiveInputTableDataContext(spark, props).getHiveInputDataContext

    logger.info("Start transformation for input data sources")

    val transformedData: Dataset[ErcsvInputData] =
      LogicUtils.transformErcsnInputData(validDs, inputDataContext.imsiMaster, batchId)
    logger.debug("%s".format(transformedData.show(truncate = false)))

    (transformedData, invalidDsWithBatch, param)
  }

}
