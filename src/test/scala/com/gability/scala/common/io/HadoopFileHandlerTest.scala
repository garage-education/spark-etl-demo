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

    val expectedValidSeq: Seq[Row] = Seq(
      Row("310120265624299", "490154203237518", "1234", "99", "1", "2020-06-15 07:45:43", etlInputTestFileName),
      Row("310120265624299", "490154203237518", "5432", "54", "2", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("310120265624299", "490154203237518", "4321", "54", "1", "2020-06-15 15:41:43", etlInputTestFileName),
      Row("310120265624299", "490154203237518", "4657", "99", "4", "2020-06-15 19:11:43", etlInputTestFileName),
      Row("310120265624299", "490154203237518", "1234", "99", "3", "2020-06-15 20:00:43", etlInputTestFileName),
      Row("310120265624234", "490154203237543", "123", "22", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("310120265624234", "490154203237543", "456", "21", "1", "2020-06-15 15:31:43", etlInputTestFileName),
      Row("310120265624234", "490154203237543", "567", "65", "2", "2020-06-15 17:53:43", etlInputTestFileName),
      Row("310120265624234", "490154203237543", "543", "66", "2", "2020-06-15 20:13:43", etlInputTestFileName),
      Row("310120265624234", "490154203237543", "4978", "33", "4", "2020-06-15 22:12:43", etlInputTestFileName),
      Row("310120265624654", "490154203237654", "4367", "22", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("310120265624123", "490154203231245", "2435", "11", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("310120265627654", "490154203235432", "1235", "43", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row(null, "3214324134", "21421", "12421", "2", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("214214", "12421412421124", null, "124", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("214214", "12421412421124", "11", null, "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("214214", "12421412421124", "11", "444", "1", null, etlInputTestFileName)
    )

    val expectedValid = spark.createDataFrame(spark.sparkContext.parallelize(expectedValidSeq), ercsnSampleInputSchemaType)

    val inputDs = readDelimitedFile(param, spark)
    assertDataFrameEquals(expectedValid, inputDs)
  }

}
