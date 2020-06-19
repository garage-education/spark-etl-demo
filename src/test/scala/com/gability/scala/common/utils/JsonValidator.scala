package com.gability.scala.common.utils

import com.gability.scala.common.metadata.Metadata.{JobParamRawDtl, SchemaDtl}
import com.gability.scala.common.utils.JsonExtractor._
import org.scalatest._

class JsonValidator extends FunSuite with Matchers {
  test("test json extractor ") {

    val expectedJsonParsed: JobParamRawDtl = JobParamRawDtl(
      "3G_ERCSN",
      "csv",
      "|",
      "5",
      "/home/moustafa/Scala/spark-etl-demo/data/raw_zone/3G_ERCSN/",
      "/data/processed/3G_ERCSN/",
      "csv",
      List("Singl_KPI"),
      "Append",
      "*_processing",
      "orc",
      "false",
      "event_date,batch_id",
      List(
        SchemaDtl("C0", "IntegerType",   true),
        SchemaDtl("C1", "StringType",    true),
        SchemaDtl("C2", "StringType",    true),
        SchemaDtl("C3", "StringType",    true),
        SchemaDtl("C4", "TimestampType", true)
      )
    )

    val actualJsonParsed: JobParamRawDtl =
      getJsonParsed[JobParamRawDtl]("config.json")
    expectedJsonParsed shouldEqual actualJsonParsed
  }
//class JsonValidatorTest extends JsonValidator[JobParamRawDtl]("config.json") {}

}
