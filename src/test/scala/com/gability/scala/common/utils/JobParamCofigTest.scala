package com.gability.scala.common.utils

import com.gability.scala.common.config.ETLConfigManagement.getPrimaryModelParams
import com.gability.scala.TestingUtils
class JobParamCofigTest extends TraitTest {
  var testingUtils: TestingUtils = _
  before {
    testingUtils = new TestingUtils(spark)
    testingUtils.prepareHiveInputTables()
  }

  test("test function getPrimaryModelParams") {
    import com.gability.scala.Constants.jobId
    spark.sql("CREATE DATABASE sandbox")
    val expectedParam = Map(
      "json" -> """{"sourceName":"3G_ERCSN","rejection":{"rejectedRecordsPath": "/home/moustafa/Scala/spark-etl-demo/data/processed/3G_ERCSN/","rejectOutputType":"csv"},"inputSource":{"inputFilesType":"csv","dataFileDelimiter":"|","totalInputFileColumns":"5","inputSourcePath":"/home/moustafa/Scala/spark-etl-demo/data/raw_zone/3G_ERCSN/","processingSuffix":"*_processing","header":"false","inputSchema":[{"columnName":"imsi","columnType":"StringType","isNullable":false},{"columnName":"imei","columnType":"StringType","isNullable":true},{"columnName":"cell","columnType":"IntegerType","isNullable":false},{"columnName":"lac","columnType":"IntegerType","isNullable":false},{"columnName":"eventType","columnType":"StringType","isNullable":true},{"columnName":"eventTs","columnType":"TimestampType","isNullable":false},{"columnName":"file_name","columnType":"StringType","isNullable":false}]},"targetSource":[{"targetTable":"Singl_KPI","targetSchema":"mod","partitionColumns":"event_date,batch_id","outputFormat":"orc","saveMode":"Append"}]}""")

    val actualParam = getPrimaryModelParams(spark, jobId, "job_config")

    actualParam shouldEqual expectedParam

  }

}
