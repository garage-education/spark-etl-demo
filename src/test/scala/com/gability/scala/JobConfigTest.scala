package com.gability.scala

import org.scalatest.{FunSuite, Matchers}
import EnvironmentConfig._
class JobConfigTest extends FunSuite with Matchers {

  test("test json extractor ") {

    val expectedParsedConfig = Conf("sandbox.job_config", "sandbox.imsi_master")

    val actualJsonParsed = parseEnvConfig("dev")

    expectedParsedConfig shouldEqual (actualJsonParsed)
  }
}
