package com.gability.scala.common.testing.utils

import com.gability.scala.common.utils.LogicUtils
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.Dataset
import org.scalatest.{FunSuite, Matchers}

class LogicUtilsTest extends FunSuite with Matchers with DatasetSuiteBase {
/*  var logic: LogicUtils = _
  override lazy val spark = SparkSessionProvider._sparkSession
  //same logic could be applied on identity data
  test("Testing readCsvWithFileNameAsColumn logic on checksdata ") {
    logic = LogicUtils(spark)

    val actual: Dataset[ChecksData] = logic.readCsvWithFileNameAsColumn[ChecksData](projectCurrentPath+checksPath, checksSchema)
    val expected: Dataset[ChecksData] = TestingUtils.getSampleChecksData.toDS()

    assertDatasetEquals(actual, expected)
  }
  //same logic could be applied on App Nationality
  test("Testing jsonMetadataParser logic on Applicant Employer Data ") {
    logic = LogicUtils(spark)

    val actual: Dataset[AppEmployerDetails] =
      logic.jsonMetadataParser[AppEmployerDetails](projectCurrentPath+employerLkpPath, Seq("applicantEmployerId", "applicantEmployerName"))

    val expected: Dataset[AppEmployerDetails] = TestingUtils.getSampleApplicantEmployerData.toDS()

    assertDatasetEquals(actual, expected)
  }*/
}
