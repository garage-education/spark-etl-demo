package com.gability.scala.common.metadata
//https://www.scala-lang.org/api/current/scala/Enumeration.html
//https://alvinalexander.com/scala/how-to-use-scala-enums-enumeration-examples/
object IOTypes extends Enumeration {
    type FType = Value
    val CSV, AVRO, PARQUET,KAFKA = Value
}