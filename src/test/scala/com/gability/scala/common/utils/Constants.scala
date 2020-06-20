package com.gability.scala.common.utils

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Constants {
  val etlInputTestFileName = "ercsn_4g_20200512182929_part02"
  val jobId = "12345678910"
  val jobName = "etl-pipeline-test"
  val batchId = "20200612152928"
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
}
