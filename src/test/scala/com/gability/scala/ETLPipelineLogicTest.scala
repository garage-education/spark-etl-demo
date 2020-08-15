package com.gability.scala

import java.sql.Timestamp
import com.gability.scala.Metadata.{Conf, ErcsvInputData}
import com.gability.scala.common.config.ETLConfigManagement.getJobConfig
import com.gability.scala.common.metadata.Metadata.JobConfig
import com.gability.scala.common.utils.{TestingUtils, TraitTest}
import com.gability.scala.common.utils.Constants._
import com.gability.scala.common.utils.EnvConfig.parseEnvConfig
import org.apache.spark.sql.{DataFrame, Row}
import pureconfig.generic.auto._

class ETLPipelineLogicTest extends TraitTest {
  var jobConfig:     JobConfig = _
  var testingUtils:  TestingUtils = _
  var jobProperties: Conf = _
  val expectedValid = Seq(
    ErcsvInputData(
      "310120265624299",
      12345,
      "4901542",
      "03237518",
      Some("490154203237518"),
      1234,
      99,
      Some("1"),
      Timestamp.valueOf("2020-06-15 07:45:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624299",
      12345,
      "4901542",
      "03237518",
      Some("490154203237518"),
      5432,
      54,
      Some("2"),
      Timestamp.valueOf("2020-06-15 12:12:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624299",
      12345,
      "4901542",
      "03237518",
      Some("490154203237518"),
      4321,
      54,
      Some("1"),
      Timestamp.valueOf("2020-06-15 15:41:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624299",
      12345,
      "4901542",
      "03237518",
      Some("490154203237518"),
      4657,
      99,
      Some("4"),
      Timestamp.valueOf("2020-06-15 19:11:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624299",
      12345,
      "4901542",
      "03237518",
      Some("490154203237518"),
      1234,
      99,
      Some("3"),
      Timestamp.valueOf("2020-06-15 20:00:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624234",
      -99999,
      "4901542",
      "03237543",
      Some("490154203237543"),
      123,
      22,
      Some("1"),
      Timestamp.valueOf("2020-06-15 12:12:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624234",
      -99999,
      "4901542",
      "03237543",
      Some("490154203237543"),
      456,
      21,
      Some("1"),
      Timestamp.valueOf("2020-06-15 15:31:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624234",
      -99999,
      "4901542",
      "03237543",
      Some("490154203237543"),
      567,
      65,
      Some("2"),
      Timestamp.valueOf("2020-06-15 17:53:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624234",
      -99999,
      "4901542",
      "03237543",
      Some("490154203237543"),
      543,
      66,
      Some("2"),
      Timestamp.valueOf("2020-06-15 20:13:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624234",
      -99999,
      "4901542",
      "03237543",
      Some("490154203237543"),
      4978,
      33,
      Some("4"),
      Timestamp.valueOf("2020-06-15 22:12:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624654",
      -99999,
      "4901542",
      "03237654",
      Some("490154203237654"),
      4367,
      22,
      Some("1"),
      Timestamp.valueOf("2020-06-15 12:12:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265624123",
      54321,
      "4901542",
      "03231245",
      Some("490154203231245"),
      2435,
      11,
      Some("1"),
      Timestamp.valueOf("2020-06-15 12:12:43"),
      batchIdLong,
      etlInputTestFileName
    ),
    ErcsvInputData(
      "310120265627654",
      -99999,
      "4901542",
      "03235432",
      Some("490154203235432"),
      1235,
      43,
      Some("1"),
      Timestamp.valueOf("2020-06-15 12:12:43"),
      batchIdLong,
      etlInputTestFileName
    )
  )
  val expectedInvalid: Seq[Row] = Seq(
    Row(null, "3214324134", "21421", "12421", "2", "2020-06-15 12:12:43", etlInputTestFileName, batchIdLong),
    Row("214214", "12421412421124", null, "124", "1", "2020-06-15 12:12:43", etlInputTestFileName, batchIdLong),
    Row("214214", "12421412421124", "11", null, "1", "2020-06-15 12:12:43", etlInputTestFileName, batchIdLong),
    Row("214214", "12421412421124", "11", "444", "1", null, etlInputTestFileName, batchIdLong)
  )

  before {
    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()
    jobConfig = getJobConfig(jobId, jobName, batchId)
    jobProperties = parseEnvConfig[Conf]("dev")
  }

  test("Testing prepare data logic ") {
    val (actualValid, actualInvalidDt, param) = ETLPipelineLogic(jobConfig, jobProperties).jobLogicRunner()
    val expectedInValid: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedInvalid), ercsnRejSchemaType)

    actualValid.collect.toSeq shouldEqual expectedValid
    assertDataFrameEquals(actualInvalidDt, expectedInValid)
    param shouldEqual expectedJsonParsed

  }
}
