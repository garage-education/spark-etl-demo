package com.gability.scala.common.utils

import com.gability.scala.common.metadata.Metadata.{JobParamRawDtl, SchemaDtl}
import com.gability.scala.common.utils.Constants.SimpleJsonObj
import com.gability.scala.common.utils.JsonExtractor._
import org.scalatest._

class JsonValidator extends TraitTest {
  test("json extractor from json string") {

    val simpleJsonStr = """{"name":"Moustafa","age":3}"""
    val expectedParsedJson = SimpleJsonObj("Moustafa", 3)
    val actualParsedJson = getJsonObj[SimpleJsonObj](simpleJsonStr)
    expectedParsedJson shouldEqual actualParsedJson

  }
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
        SchemaDtl("imsi", "StringType", isNullable = false),
        SchemaDtl("imei", "StringType", isNullable = true),
        SchemaDtl("cell", "IntegerType", isNullable = false),
        SchemaDtl("lac", "IntegerType", isNullable = false),
        SchemaDtl("eventType", "StringType", isNullable = true),
        SchemaDtl("eventTs", "TimestampType", isNullable = false),
        SchemaDtl("fileName", "StringType", isNullable = false)
      )
    )

    val actualJsonParsed: JobParamRawDtl =
      getJsonParsedFromFile[JobParamRawDtl]("config.json")
    expectedJsonParsed shouldEqual actualJsonParsed
  }
//class JsonValidatorTest extends JsonValidator[JobParamRawDtl]("config.json") {}

}
