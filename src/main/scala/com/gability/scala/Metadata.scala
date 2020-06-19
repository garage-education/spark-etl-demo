package com.gability.scala

import java.sql.Timestamp

import org.apache.spark.sql.types._

object Metadata {

  val ercsnStructSchema: StructType =
    StructType(
      StructField("imsi", StringType, nullable = false) ::
        StructField("imei", StringType, nullable = true) ::
        StructField("cell", IntegerType, nullable = false) ::
        StructField("lac", IntegerType, nullable = false) ::
        StructField("eventType", StringType, nullable = true) ::
        StructField("eventTs", TimestampType, nullable = false) ::
        StructField("fileName", StringType, nullable = true) :: Nil
    )

  case class InputRow(imsi: String, imei: String, cell: Int, lac: Int, eventType: String, eventTs: Timestamp, fileName: String)
}
