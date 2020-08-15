package com.gability.scala

import com.gability.scala.common.utils.TraitTest
import com.gability.scala.Constants.{batchId, env, expectedJsonParsed, expectedValid, jobId, jobName}
import com.gability.scala.Metadata.ErcsvInputData
import org.apache.spark.sql.{DataFrame, Dataset}

class MainTest extends TraitTest {
  val COMMAND_LINE_RUN = Array(jobId, jobName, batchId, env)
  var testingUtils: TestingUtils = _
  before {
    spark.sql("CREATE DATABASE mod")
    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()
  }
  test("Testing Main class ") {
    import spark.implicits._

    Main.main(COMMAND_LINE_RUN)
    val tgtSource = expectedJsonParsed.targetSource.head
    val actualInsertedData: Dataset[ErcsvInputData] = spark
      .table(tgtSource.targetSchema + "." + tgtSource.targetTable)
      .as[ErcsvInputData]

    actualInsertedData.collect().toSeq shouldEqual expectedValid

  }
}
