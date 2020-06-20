package com.gability.scala

import com.gability.scala.Metadata.ErcsvInputData
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, TimestampType}

object LogicUtils extends Logging {
  def transformErcsnInputData(inputDs: Dataset[Row], imsiMaster: Dataset[Row], batchId: Long): Dataset[ErcsvInputData] = {
    import inputDs.sparkSession.implicits._

    logger.info("transform input data frame")
    val inputDsJoinedImsi = inputDs
      .join(broadcast(imsiMaster), Seq("imsi"), "left_outer")
      .select(
        'imsi,
        when('subscriber_Id.isNull, -99999) otherwise 'subscriber_Id.cast(LongType) as "subscriberId",
        when(length('imei) < 15, "-99999") otherwise substring('imei, 0, 7) as "tac",
        when(length('imei) < 15, "-99999") otherwise substring('imei, 8, 13) as "snr",
        'imei,
        'cell.cast(IntegerType) as "cell",
        'lac.cast(IntegerType) as "lac",
        'eventType,
        'eventTs.cast(TimestampType),
        lit(batchId) as "batchId",
        'fileName
      )
      .as[ErcsvInputData]

    logger.debug("%s".format(inputDsJoinedImsi.show))
    inputDsJoinedImsi
  }
}
