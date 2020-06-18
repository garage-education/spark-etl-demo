package com.gability.scala.common.utils

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import EtlUtils._
import com.gability.scala.Metadata._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import Constants._
import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.JsonExtractor.getJsonParsed
class ETLUtilsTest extends FunSuite with Matchers with BeforeAndAfter with DatasetSuiteBase {

  val inputSampleData: Seq[(String, String, String, String, String, String)] = Seq(
    ("12456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
    ("22456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
    ("32456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
    ("42456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
    ("SSSSSS", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
    ("8765", null, null, null, null, etlInputTestFileName),
    ("SSSSS", null, null, null, null, etlInputTestFileName)
  )
  val colName = List("_c0", "_c1", "_c2", "_c3", "_c4", "file_name")

  test("test read valid delimited file ") {
    import spark.implicits._
    val inputDs = inputSampleData.toDF(colName: _*)

    val (actualValidDt, _) = validateDataset(inputDs, ercsnStructSchema)

    val expectedValidSeq: Seq[Row] = Seq(
      Row("12456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("22456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("32456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("42456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("8765", null, null, null, null, etlInputTestFileName)
    )
    val expectedValid: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedValidSeq), ercsnSampleInputSchemaType)

    assertDatasetEquals(expectedValid, actualValidDt)
  }

  test("test read inValid delimited file ") {
    import spark.implicits._

    val inputDs = inputSampleData.toDF(colName: _*)

    val (_, actualInvalidDt: Dataset[Row]) = validateDataset(inputDs, ercsnStructSchema)

    val expectedInvalid: Seq[Row] = Seq(
      Row("SSSSSS", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("SSSSS", null, null, null, null, etlInputTestFileName)
    )

    actualInvalidDt.collect.toSeq shouldEqual (expectedInvalid)

  }

}
