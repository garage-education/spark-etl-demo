package com.gability.scala

import java.sql.Timestamp

import Constants.etlInputTestFileName
import com.gability.scala.common.utils.TraitTest
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import Constants._
import com.gability.scala.Metadata.ErcsvInputData
class LogicUtilsTest extends TraitTest {

  var testingUtils: TestingUtils = _
  before {
    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()
  }

  val inputSampleData: Seq[Row] = Seq(
    Row("310120265624299", "490154203237518", "1234", "99", "1", "2020-06-15 07:45:43", etlInputTestFileName),
    Row("310120265624299", "490154203237518", "5432", "54", "2", "2020-06-15 12:12:43", etlInputTestFileName),
    Row("310120265624234", "490154203237543", "123", "22", "1", "2020-06-15 12:12:43", etlInputTestFileName),
    Row("310120265624123", "490154203231245", "2435", "11", "1", "2020-06-15 12:12:43", etlInputTestFileName),
    Row("310120265624123", null, "2435", "11", "1", "2020-06-15 12:12:43", etlInputTestFileName)
  )
  val colName = ercsnSchemaType.map(_.name)
  val batchIdLong = batchId.toLong

  test("test transformErcsnInputData ") {
    val inputDs: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(inputSampleData),
      ercsnSchemaType
    )
    val imsiMaster = spark.table("imsi_master")
    val acutalData =
      LogicUtils.transformErcsnInputData(inputDs, imsiMaster, batchIdLong)

    val expectedDs = Seq(
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
        "310120265624123",
        54321,
        null,
        null,
        None,
        2435,
        11,
        Some("1"),
        Timestamp.valueOf("2020-06-15 12:12:43"),
        batchIdLong,
        etlInputTestFileName
      )
    )

    acutalData.collect().toSeq should contain theSameElementsAs expectedDs
  }
}
