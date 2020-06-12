package com.gability.scala

import com.gability.scala.common.config.ETLConfigManagement.getJobConfig
import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.testing.utils.TestingUtils
import com.gability.scala.common.utils.HiveUtils
import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest._

class MainTest
    extends FunSuite
    with Matchers
    with DatasetSuiteBase
    with BeforeAndAfter {
  var jobConfig: JobConfig = _
  var testingUtils:TestingUtils = _
  before {
    val jobId = "12345678910"
    val jobName = "etl-pipeline-test"
    import spark.implicits._
    testingUtils= new TestingUtils(spark)
    testingUtils.prepareHiveInputTables

    jobConfig = getJobConfig(jobId, jobName)
  }

  test("Testing prepare data logic ") {
    import spark.implicits._
    spark.table("job_config").show()
  }
}
