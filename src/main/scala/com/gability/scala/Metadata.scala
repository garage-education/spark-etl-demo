package com.gability.scala

import java.sql.Timestamp

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object Metadata {

  val ercsnStructSchema: StructType =
    StructType(
      StructField("a", IntegerType, false) ::
        StructField("b", StringType, true) ::
        StructField("c", StringType, true) ::
        StructField("d", StringType, true) ::
        StructField("e", TimestampType, true) ::
        StructField("f", StringType, true) :: Nil
    )

  case class ErcsvInputData(custId: String, custName: String, custCity: String)

  final case class InputRow(a: Option[Int], b: String, c: String, d: String, e: Timestamp, f: String)
}
