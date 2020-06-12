package com.gability.scala.common.testing.utils

import com.gability.scala.common.utils.HiveUtils
import org.apache.spark.sql.{Row, SparkSession}

class TestingUtils(spark: SparkSession) {
  var hiveUtils = new HiveUtils(spark)
  def prepareHiveInputTables(): Unit = {
    hiveUtils.createTmpHiveTableWithDefaultName("job_config")
  }
}
