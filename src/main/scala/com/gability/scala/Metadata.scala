package com.gability.scala

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

object Metadata {

  val ercsnStructSchema: StructType =
    StructType(
      StructField("imsi", StringType, nullable = false) ::
        StructField("imei", StringType, nullable = true) ::
        StructField("cell", IntegerType, nullable = false) ::
        StructField("lac", IntegerType, nullable = false) ::
        StructField("eventType", StringType, nullable = true) ::
        StructField("eventTs", TimestampType, nullable = false) ::
        StructField("file_name", StringType, nullable = true) :: Nil
    )

  case class Conf(configParam: String, imsiMaster: String)

  case class InputDataContext(imsiMaster: Dataset[Row])

  case class ErcsvInputData(imsi:         String,
                            subscriberId: Long,
                            tac:          String,
                            snr:          String,
                            imei:         Option[String],
                            cell:         Int,
                            lac:          Int,
                            eventType:    Option[String],
                            eventTs:      Timestamp,
                            file_name:    String,
                            event_date:   Date,
                            batch_id:     Long)

  case class HiveInputDataContext(configParam: Dataset[Row], imsiMaster: Dataset[Row])
}
