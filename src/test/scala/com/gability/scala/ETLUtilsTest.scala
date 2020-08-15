package com.gability.scala

import com.gability.scala.common.utils.EtlUtils._
import com.gability.scala.Constants._
import com.gability.scala.Metadata._
import com.gability.scala.common.utils.TraitTest
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class ETLUtilsTest extends TraitTest {

  val inputSampleData: Seq[(String, String, String, String, String, String, String)] = Seq(
    ("310120265624299", "490154203237518", "1234", "99", "1", "2020-06-15 07:45:43", etlInputTestFileName),
    ("310120265624299", "490154203237518", "5432", "54", "2", "2020-06-15 12:12:43", etlInputTestFileName),
    ("310120265624234", "490154203237543", "123", "22", "1", "2020-06-15 12:12:43", etlInputTestFileName),
    ("310120265624123", "490154203231245", "2435", "11", "1", "2020-06-15 12:12:43", etlInputTestFileName),
    ("310120265624123", null, "2435", "11", "1", "2020-06-15 12:12:43", etlInputTestFileName),
    (null, "3214324134", "21421", "12421", "2", "2020-06-15 12:12:43", etlInputTestFileName),
    ("214214", "12421412421124", null, "124", "1", "2020-06-15 12:12:43", etlInputTestFileName),
    ("214214", "12421412421124", "11", null, "1", "2020-06-15 12:12:43", etlInputTestFileName),
    ("214214", "12421412421124", "11", "444", "1", null, etlInputTestFileName)
  )
  val colName = ercsnSchemaType.map(_.name)

  test("test read valid delimited file ") {
    import spark.implicits._
    val inputDs = inputSampleData.toDF(colName: _*)

    val (actualValidDt, _) = validateDataset(inputDs, ercsnStructSchema)

    val expectedValidSeq: Seq[Row] = Seq(
      Row("310120265624299", "490154203237518", "1234", "99", "1", "2020-06-15 07:45:43", etlInputTestFileName),
      Row("310120265624299", "490154203237518", "5432", "54", "2", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("310120265624234", "490154203237543", "123", "22", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("310120265624123", "490154203231245", "2435", "11", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("310120265624123", null, "2435", "11", "1", "2020-06-15 12:12:43", etlInputTestFileName)
    )
    val expectedValid: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedValidSeq), ercsnSchemaType)

    assertDatasetEquals(expectedValid, actualValidDt)
  }

  test("test read inValid delimited file ") {
    import spark.implicits._

    val inputDs = inputSampleData.toDF(colName: _*)

    val (_, actualInvalidDt: Dataset[Row]) = validateDataset(inputDs, ercsnStructSchema)

    val expectedInvalid: Seq[Row] = Seq(
      Row(null, "3214324134", "21421", "12421", "2", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("214214", "12421412421124", null, "124", "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("214214", "12421412421124", "11", null, "1", "2020-06-15 12:12:43", etlInputTestFileName),
      Row("214214", "12421412421124", "11", "444", "1", null, etlInputTestFileName)
    )
    val expectedInValid: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedInvalid), ercsnSchemaType)
    actualInvalidDt.collect.toSeq shouldEqual expectedInvalid
    assertDatasetEquals(expectedInValid, actualInvalidDt)

  }

}
