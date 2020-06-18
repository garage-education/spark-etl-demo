package com.gability.scala.common.io

import java.sql.{Date, Timestamp}

import com.gability.scala.common.io.HadoopFileHandler._
import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.JsonExtractor.getJsonParsed
import com.gability.scala.Metadata.ercsnStructSchema
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class HadoopFileHandlerTest extends FunSuite with Matchers with BeforeAndAfter with DatasetSuiteBase {

  var param: JobParamRawDtl = _
  val fileName = "ercsn_3g_20200512182929_part01"
  before {
    param = getJsonParsed[JobParamRawDtl]("config.json")
  }

  test("test read valid delimited file ") {
    import spark.implicits._

    val (actualValidDt: Dataset[Row], _) = readDelimitedFile(param, ercsnStructSchema, spark)

    val expectedValidSeq: Seq[Row] = Seq(
      Row("12456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      Row("22456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      Row("32456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      Row("42456", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      Row("8765", null, null, null, null, fileName)
    )

    val expectedValid = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedValidSeq),
      StructType(
        StructField("_c0", StringType, true) ::
          StructField("_c1", StringType, true) ::
          StructField("_c2", StringType, true) ::
          StructField("_c3", StringType, true) ::
          StructField("_c4", StringType, true) ::
          StructField("file_name", StringType, true) :: Nil
      )
    )

    //TODO: fix a bug in case nullable failed with null value will fail while parsing
    //TODO: remove for example STRING3 from the file
    assertDataFrameEquals(expectedValid, actualValidDt)
    //actualValidDt.collect.toList should contain theSameElementsAs (expectedValid)
  }

  test("test read inValid delimited file ") {

    val (_, actualInvalidDt: Dataset[Row]) = readDelimitedFile(param, ercsnStructSchema, spark)

    val expectedInvalid: Seq[Row] = Seq(
      Row("SSSSSS", "STRING1", "STRING2", "STRING3", "2020-04-03 20:15:14", fileName),
      Row("42456", "STRING1", null, null, null, fileName),
      Row("SSSSS", null, null, null, null, fileName)
    )

    actualInvalidDt.collect.toSeq shouldEqual (expectedInvalid)

  }

}
