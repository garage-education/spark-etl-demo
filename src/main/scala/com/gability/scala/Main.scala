package com.gability.scala

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.metadata.Metadata
import org.apache.log4j.{Level, Logger}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import common.config.ETLConfigManagement._

object Main extends Logging {

  //TODO: create generic way for main args
  def main(args: Array[String]): Unit = {
    if (args.length >= 2) {
      val appStartingTS = LocalDateTime.now()
      logger.info("Application Started ..")

      logger.info("Initialize Spark Session ..")
      val jobConfig: JobConfig = getJobConfig(args(0), args(1))

      logger.debug("Turn off spark internal logging ..")
      //TODO: get spark logging from log2j.xml
      Logger.getLogger("org").setLevel(Level.OFF) //turn org libs additional logs
      Logger.getLogger("akka").setLevel(Level.OFF) //turn off akka logs

      logger.info("Start Transformation Pipeline ..")
      ETLPipelineLogic(jobConfig).jobLogicRunner

      //TODO: choose better way for logging the actual job time.
      logger.info("elapsed time: " + appStartingTS.until(LocalDateTime.now(), ChronoUnit.SECONDS) + "s")
    } else {
      val usage = """ Usage: spark-submit pipeline-0.1-jar-with-dependencies.jar job_id job_name """
      println(usage)
    }

  }
}
