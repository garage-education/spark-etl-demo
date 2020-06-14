package com.gability.scala

import java.sql.Timestamp

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object Metadata {

  val ercsnStructSchema: StructType =
    StructType(
      StructField("a", IntegerType, true) ::
        StructField("b", StringType, false) ::
        StructField("c", StringType, false) ::
        StructField("d", StringType, false) ::
        StructField("e", TimestampType, false) ::
        StructField("f", StringType, false) :: Nil
    )

  case class ErcsvInputData(custId: String, custName: String, custCity: String)
  final case class InputRow(a: Int, b: String, c: String, d: String, e: Timestamp)
}
