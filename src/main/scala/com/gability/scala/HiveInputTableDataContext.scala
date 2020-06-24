package com.gability.scala

import com.gability.scala.Metadata._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HiveInputTableDataContext(spark: SparkSession, config: Conf) extends Logging {

  def getHiveInputDataContext: HiveInputDataContext = {
    logger.info("register table config_param")
    val configParam: DataFrame = spark.table(config.configParam)
    logger.debug("%s".format(configParam.show))

    logger.info("register table imsi_master")
    val imsiMaster = spark.table(config.configParam)
    logger.debug("%s".format(imsiMaster.show()))

    HiveInputDataContext(configParam, imsiMaster)

  }

}
