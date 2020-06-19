package com.gability.scala.common.utils

import com.gability.scala.common.io.HiveUtils
import org.apache.spark.sql.SparkSession

class TestingUtils(spark: SparkSession) {
  var hiveUtils = new HiveUtils(spark)
  def prepareHiveInputTables(): Unit = {
    hiveUtils.createTmpHiveTableWithDefaultName("job_config")
    hiveUtils.createTmpHiveTableWithDefaultName("imsi_master")
  }
}
