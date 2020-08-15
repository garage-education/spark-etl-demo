package com.gability.scala.common.utils

import com.gability.scala.common.metadata.Metadata.{InputSource, JobParamRawDtl, Rejection, SchemaDtl, TargetSource}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Constants {
  val etlInputTestFileName = "ercsn_4g_20200512182929_part02"
  val jobId = "12345678910"
  val jobName = "etl-pipeline-test"
  val batchId = "20200612152928"
  val batchIdLong = 20200612152928L
  case class SimpleJsonObj(name: String, age: Int)

  val ercsnSchemaType = StructType(
    StructField("imsi", StringType, nullable = true) ::
      StructField("imei", StringType, nullable = true) ::
      StructField("cell", StringType, nullable = true) ::
      StructField("lac", StringType, nullable = true) ::
      StructField("eventType", StringType, nullable = true) ::
      StructField("eventTs", StringType, nullable = true) ::
      StructField("fileName", StringType, nullable = true) :: Nil
  )

  val ercsnRejSchemaType = StructType(
    StructField("imsi", StringType, nullable = true) ::
      StructField("imei", StringType, nullable = true) ::
      StructField("cell", StringType, nullable = true) ::
      StructField("lac", StringType, nullable = true) ::
      StructField("eventType", StringType, nullable = true) ::
      StructField("eventTs", StringType, nullable = true) ::
      StructField("fileName", StringType, nullable = true) ::
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
        SchemaDtl("fileName", "StringType", isNullable = false)
      )
    ),
    Rejection(
      "/data/processed/3G_ERCSN/",
      "csv"
    ),
    List(TargetSource("Singl_KPI", "mod", "event_date,batch_id", "orc", "Append"))
  )
}
