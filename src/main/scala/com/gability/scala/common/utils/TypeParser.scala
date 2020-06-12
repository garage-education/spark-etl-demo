package com.gability.scala.common.utils

import scala.util.Try

trait ParseIt[T] {
  protected def parse(s: String): T
  def apply(s: String): Option[T] = Try(parse(s)).toOption
}

object TypeParser {
  implicit object ParseInt extends ParseIt[Int] {
    protected def parse(s: String): Int = s.toInt
  }

  implicit object ParseDouble extends ParseIt[Double] {
    protected def parse(s: String): Double = s.toDouble
  }

  implicit object ParseLong extends ParseIt[Long] {
    protected def parse(s: String): Long = s.toLong
  }

  def parseString[T: ParseIt](s: String, orElse: => T): T =
    implicitly[ParseIt[T]].apply(s).getOrElse(orElse)

}
