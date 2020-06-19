package com.gability.scala

import com.gability.scala.common.config.ETLConfigManagement.getJobConfig
import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.utils.TestingUtils
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest._
import com.gability.scala.common.utils.Constants._
class MainTest extends FunSuite with Matchers with DatasetSuiteBase with BeforeAndAfter {
  var jobConfig:    JobConfig = _
  var testingUtils: TestingUtils = _
  before {
    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()

    jobConfig = getJobConfig(jobId, jobName, batchId)
  }

  test("Testing prepare data logic ") {
    spark.table("job_config").show()
  }
}
