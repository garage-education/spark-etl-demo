package com.gability.scala.common.io

import com.gability.scala.common.io.HadoopFileHandler._
import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.Constants._
import com.gability.scala.common.utils.JsonExtractor.getJsonParsed
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class HadoopFileHandlerTest extends FunSuite with Matchers with BeforeAndAfter with DatasetSuiteBase {

  var param: JobParamRawDtl = _

  before {
    param = getJsonParsed[JobParamRawDtl]("config.json")
  }

  test("test read valid delimited file ") {

    val inputDs = readDelimitedFile(param, spark)

    val expectedValidSeq: Seq[Row] = Seq(
      Row("12456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("22456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("32456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("42456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("SSSSSS", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", etlInputTestFileName),
      Row("8765", null, null, null, null, etlInputTestFileName),
      Row("SSSSS", null, null, null, null, etlInputTestFileName)
    )

    val expectedValid = spark.createDataFrame(spark.sparkContext.parallelize(expectedValidSeq), ercsnSampleInputSchemaType)

    assertDataFrameEquals(expectedValid, inputDs)
  }

}
