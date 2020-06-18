package com.gability.scala.common.utils

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

object Constants {
  val etlInputTestFileName = "ercsn_3g_20200512182929_part01"

  val ercsnSampleInputSchemaType = StructType(
    StructField("_c0", StringType, true) ::
      StructField("_c1", StringType, true) ::
      StructField("_c2", StringType, true) ::
      StructField("_c3", StringType, true) ::
      StructField("_c4", StringType, true) ::
      StructField("file_name", StringType, true) :: Nil
  )
}
