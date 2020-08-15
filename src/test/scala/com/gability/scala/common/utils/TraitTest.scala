package com.gability.scala.common.utils

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

trait TraitTest extends FunSuite with Matchers with BeforeAndAfter with DatasetSuiteBase
