package com.gability.scala.common.utils

import com.gability.scala.common.metadata.Metadata.{JobParamRawDtl, SchemaDtl}
import com.gability.scala.Constants.{expectedJsonParsed, SimpleJsonObj}
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

    val actualJsonParsed: JobParamRawDtl =
      getJsonParsedFromFile[JobParamRawDtl]("config.json")
    actualJsonParsed shouldEqual expectedJsonParsed
  }
}
