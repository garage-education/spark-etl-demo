package com.gability.scala

import java.sql.{Date, Timestamp}

import com.gability.scala.common.metadata.Metadata._
import com.gability.scala.Metadata.ErcsvInputData
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object Constants {
  val inputTestFile = "ercsn_4g_20200512182929_part02"
  val jobId = "12345678910"
  val jobName = "etl-pipeline-test"
  val batchId = "20200612152928"
  val env = "dev"
  val batchIdLong = 20200612152928L
  case class SimpleJsonObj(name: String, age: Int)

  val ercsnSchemaType = StructType(
    StructField("imsi", StringType, nullable = true) ::
      StructField("imei", StringType, nullable = true) ::
      StructField("cell", StringType, nullable = true) ::
      StructField("lac", StringType, nullable = true) ::
      StructField("eventType", StringType, nullable = true) ::
      StructField("eventTs", StringType, nullable = true) ::
      StructField("file_name", StringType, nullable = true) :: Nil
  )

  val ercsnRejSchemaType = StructType(
    StructField("imsi", StringType, nullable = true) ::
      StructField("imei", StringType, nullable = true) ::
      StructField("cell", StringType, nullable = true) ::
      StructField("lac", StringType, nullable = true) ::
      StructField("eventType", StringType, nullable = true) ::
      StructField("eventTs", StringType, nullable = true) ::
      StructField("file_name", StringType, nullable = true) ::
      StructField("batch_id", LongType, nullable = false) :: Nil
  )

  val expectedJsonParsed: JobParamRawDtl = JobParamRawDtl(
    "3G_ERCSN",
    InputSource(
      "csv",
      "|",
      "5",
      "/home/moustafa/Scala/spark-etl-demo/data/raw_zone/3G_ERCSN/",
      "*_processing",
      "false",
      List(
        SchemaDtl("imsi", "StringType", isNullable = false),
        SchemaDtl("imei", "StringType", isNullable = true),
        SchemaDtl("cell", "IntegerType", isNullable = false),
        SchemaDtl("lac", "IntegerType", isNullable = false),
        SchemaDtl("eventType", "StringType", isNullable = true),
        SchemaDtl("eventTs", "TimestampType", isNullable = false),
        SchemaDtl("file_name", "StringType", isNullable = false)
      )
    ),
    Rejection(
      "/home/moustafa/Scala/spark-etl-demo/data/processed/3G_ERCSN/",
      "csv"
    ),
    List(TargetSource("Singl_KPI", "mod", "event_date,batch_id", "orc", "Append"))
  )

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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
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
      inputTestFile,
      Date.valueOf("2020-06-15"),
      batchIdLong
    )
  )

  val expectedInvalid: Seq[Row] = Seq(
    Row(null, "3214324134", "21421", "12421", "2", "2020-06-15 12:12:43", inputTestFile),
    Row("214214", "12421412421124", null, "124", "1", "2020-06-15 12:12:43", inputTestFile),
    Row("214214", "12421412421124", "11", null, "1", "2020-06-15 12:12:43", inputTestFile),
    Row("214214", "12421412421124", "11", "444", "1", null, inputTestFile)
  )
}
