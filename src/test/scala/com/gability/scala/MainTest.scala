package com.gability.scala

import com.gability.scala.common.config.ETLConfigManagement.getJobConfig
import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.utils.{HiveUtils, TestingUtils}
import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest._

class MainTest extends FunSuite with Matchers with DatasetSuiteBase with BeforeAndAfter {
  var jobConfig: JobConfig = _
  var testingUtils: TestingUtils = _
  before {
    val jobId = "12345678910"
    val jobName = "etl-pipeline-test"
    val batchId = "20200612152928"
    import spark.implicits._
    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()

    jobConfig = getJobConfig(jobId, jobName, batchId)
  }

  test("Testing prepare data logic ") {
    spark.table("job_config").show()
  }
}
