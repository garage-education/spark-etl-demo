package com.gability.scala

import com.gability.scala.common.config.ETLConfigManagement._
import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.utils.EnvConfig.parseEnvConfig
import com.gability.scala.Metadata.Conf
import org.apache.logging.log4j.scala.Logging
import pureconfig.generic.auto._
object Main extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length >= 3) {
      logger.info("Application Started ..")

      logger.info("Initialize Spark Session ..")
      val jobConfig: JobConfig = getJobConfig(args(0), args(1), args(2))

      val jobProperties: Conf = parseEnvConfig[Conf](args(3))

      logger.info("Start Transformation Pipeline ..")
      val (valid, invalid) = ETLPipelineLogic(jobConfig, jobProperties).jobLogicRunner()

    } else {
      val usage = """ Usage: spark-submit pipeline-0.1-jar-with-dependencies.jar job_id job_name """
      println(usage)
    }

  }
}
