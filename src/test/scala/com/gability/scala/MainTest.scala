package com.gability.scala

import com.gability.scala.Metadata.Conf
import com.gability.scala.common.config.ETLConfigManagement.getJobConfig
import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.utils.TestingUtils
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest._
import com.gability.scala.common.utils.Constants._
import com.gability.scala.common.utils.EnvConfig.parseEnvConfig
import pureconfig.generic.auto._
class MainTest extends FunSuite with Matchers with DatasetSuiteBase with BeforeAndAfter {
  var jobConfig:     JobConfig = _
  var testingUtils:  TestingUtils = _
  var jobProperties: Conf = _
  before {
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\bin\\winutils.exe")

    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()
    jobConfig = getJobConfig(jobId, jobName, batchId)
    jobProperties = parseEnvConfig[Conf]("dev")
    println(jobProperties)
  }

  test("Testing prepare data logic ") {
    ETLPipelineLogic(jobConfig, jobProperties).jobLogicRunner()
  }
}
