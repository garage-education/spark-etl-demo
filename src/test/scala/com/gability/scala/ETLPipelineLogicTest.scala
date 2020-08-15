package com.gability.scala

import java.sql.{Date, Timestamp}

import com.gability.scala.Metadata.{Conf, ErcsvInputData}
import com.gability.scala.common.config.ETLConfigManagement.getJobConfig
import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.utils.TraitTest
import Constants._
import com.gability.scala.common.utils.EnvConfig.parseEnvConfig
import org.apache.spark.sql.{DataFrame, Row}
import pureconfig.generic.auto._

class ETLPipelineLogicTest extends TraitTest {
  var jobConfig:     JobConfig = _
  var testingUtils:  TestingUtils = _
  var jobProperties: Conf = _

  val expectedInvalid: Seq[Row] = Seq(
    Row(null, "3214324134", "21421", "12421", "2", "2020-06-15 12:12:43", inputTestFile, batchIdLong),
    Row("214214", "12421412421124", null, "124", "1", "2020-06-15 12:12:43", inputTestFile, batchIdLong),
    Row("214214", "12421412421124", "11", null, "1", "2020-06-15 12:12:43", inputTestFile, batchIdLong),
    Row("214214", "12421412421124", "11", "444", "1", null, inputTestFile, batchIdLong)
  )

  before {
    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()
    jobConfig = getJobConfig(jobId, jobName, batchId)
    jobProperties = parseEnvConfig[Conf](env)
  }

  test("Testing prepare data logic ") {
    val (actualValid, actualInvalidDt, param) = ETLPipelineLogic(jobConfig, jobProperties).jobLogicRunner()
    val expectedInValid: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedInvalid), ercsnRejSchemaType)

    actualValid.collect.toSeq shouldEqual expectedValid
    assertDataFrameEquals(actualInvalidDt, expectedInValid)
    param shouldEqual expectedJsonParsed

  }
}
