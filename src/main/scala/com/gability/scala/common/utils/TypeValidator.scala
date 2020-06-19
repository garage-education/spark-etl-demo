package com.gability.scala.common.utils

import java.sql.{Date, Timestamp}
import scala.util.Try

object TypeValidator {

  case class ParseOp[T](op: String => T)
  implicit val validateDouble:    ParseOp[Double]    = ParseOp[Double](_.toDouble)
  implicit val validateInt:       ParseOp[Int]       = ParseOp[Int](_.toInt)
  implicit val validateTimestamp: ParseOp[Timestamp] = ParseOp[Timestamp](Timestamp.valueOf)
  implicit val validateString:    ParseOp[String]    = ParseOp[String](_.toString)
  implicit val validateByte:      ParseOp[Byte]      = ParseOp[Byte](_.toByte)
  implicit val validateShort:     ParseOp[Short]     = ParseOp[Short](_.toShort)
  implicit val validateLong:      ParseOp[Long]      = ParseOp[Long](_.toLong)
  implicit val validateFloat:     ParseOp[Float]     = ParseOp[Float](_.toFloat)
  implicit val validateBoolean:   ParseOp[Boolean]   = ParseOp[Boolean](_.toBoolean)
  implicit val validateDate:      ParseOp[Date]      = ParseOp[Date](Date.valueOf)

  def parse[T: ParseOp](s: String): Boolean = Try { implicitly[ParseOp[T]].op(s) }.isSuccess

}
