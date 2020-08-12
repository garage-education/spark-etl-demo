package com.gability.scala.common.utils

import com.gability.scala.common.utils.EnvConfig._
import com.gability.scala.Metadata.Conf
import org.scalatest.{FunSuite, Matchers}
import pureconfig.generic.auto._
class EnvConfigTest extends FunSuite with Matchers {

  test("test json extractor ") {

    val expectedParsedConfig = Conf("job_config", "imsi_master")

    val actualJsonParsed = parseEnvConfig[Conf]("dev")

    expectedParsedConfig shouldEqual (actualJsonParsed)
  }
}
