package com.gability.scala.common.io

import com.gability.scala.common.io.HadoopFileHandler._
import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.JsonExtractor.getJsonParsed
import com.gability.scala.Metadata.ercsnStructSchema
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class HadoopFileHandlerTest extends FunSuite with Matchers with BeforeAndAfter with DatasetSuiteBase {

  var param: JobParamRawDtl = _

  before {
    param = getJsonParsed[JobParamRawDtl]("config.json")
  }

  test("test read delimited file ") {
    import spark.implicits._

    val (actualValidDt: Dataset[Row], actualInvalidDt: Dataset[Row]) = readDelimitedFile(param, ercsnStructSchema, spark)
    val fileName = "ercsn_3g_20200512182929_part01"
    val expectedInvalid: Seq[Row] = Seq(
      Row("SSSSSS", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      Row("42456", "STRING1", null, null, null, fileName),
      Row("SSSSS", null, null, null, null, fileName)
    )

    val expectedValid: Dataset[Row] = Seq(
      (12456, "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      (22456, "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      (32456, "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      (42456, "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName)
    ).toDF("_c0", "_c1", "_c2", "_c3", "_c4", "file_name")

    actualInvalidDt.collect.toSeq shouldEqual (expectedInvalid)
    assertDataFrameDataEquals(expectedValid, actualValidDt)
  }

}
