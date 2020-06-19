package com.gability.scala.common.utils

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Constants {
  val etlInputTestFileName = "ercsn_4g_20200512182929_part02"

  val ercsnSampleInputSchemaType = StructType(
    StructField("_c0", StringType, nullable = true) ::
      StructField("_c1", StringType, nullable = true) ::
      StructField("_c2", StringType, nullable = true) ::
      StructField("_c3", StringType, nullable = true) ::
      StructField("_c4", StringType, nullable = true) ::
      StructField("_c5", StringType, nullable = true) ::
      StructField("fileName", StringType, nullable = true) :: Nil
  )

  val ercsnSchemaTypeRenamed = StructType(
    StructField("imsi", StringType, nullable = true) ::
      StructField("imei", StringType, nullable = true) ::
      StructField("cell", StringType, nullable = true) ::
      StructField("lac", StringType, nullable = true) ::
      StructField("eventType", StringType, nullable = true) ::
      StructField("eventTs", StringType, nullable = true) ::
      StructField("fileName", StringType, nullable = true) :: Nil
  )
}
