package com.gability.scala.common.utils

import java.sql.Timestamp
import java.sql.Date

import com.gability.scala.common.utils.JsonExtractor._
import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import org.apache.spark.sql.{Encoders, Row, SaveMode}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.util.Try
import scala.reflect.runtime.{universe => ru}
object EtlUtils {

  /** getJsonObj parse json multi-line or one line string.
    *
    * @param T          : subtype extended from JobParamRawDtl
    * @param jsonString : String represent json string
    * @param m          : Manifest is an implicit param for parsing json to T
    * @return T object represent json case class
    * @see [[``com.gability.scala.common.metadata.Metadata.JobParamRawDtl``]]
    */
  def getInputFileParam[T <: JobParamRawDtl](jsonString: String)(implicit m: Manifest[T]): T = {
    getJsonObj[T](jsonString)
  }

  def schemaParser(a: Row, fields: Seq[String => Boolean]): Boolean = {
    if (fields.size != a.size) false
    else {
      (0 until a.size) zip fields forall {
        case (idx, f) =>
          val cell = a.getAs[String](idx)
          f.apply(cell)
      }
    }
  }

  def structSchemaValidator(struct: StructType): Seq[String => Boolean] =
    struct map { x =>
      if (x.nullable) {
        nullableValidator(validator(x.dataType))
      } else
        validator(x.dataType)
    }

  def schema2CaseClassValidator[T <: Product: ru.TypeTag](): Seq[String => Boolean] = {
    val personEncoder = Encoders.product[T]
    val personSchema: StructType = personEncoder.schema
    structSchemaValidator(personSchema)
  }

  val validator: Map[DataType, String => Boolean] = Map(
    StringType -> validateString,
    IntegerType -> validateInt,
    TimestampType -> validateTimestamp,
    ShortType -> validateShort,
    LongType -> validateLong,
    FloatType -> validateFloat,
    DoubleType -> validateDouble,
    BooleanType -> validateBoolean,
    DateType -> validateDate
  )

  def nullableValidator(validator: String => Boolean): String => Boolean = a => a == null || validator(a)

  def validateInt(a: String): Boolean = Try(a.toInt).isSuccess
  def validateTimestamp(a: String): Boolean = Try(Timestamp.valueOf(a)).isSuccess
  def validateString(a: String): Boolean = true
  def validateByte(a: String): Boolean = Try(a.toByte).isSuccess
  def validateShort(a: String): Boolean = Try(a.toShort).isSuccess
  def validateLong(a: String): Boolean = Try(a.toLong).isSuccess
  def validateFloat(a: String): Boolean = Try(a.toFloat).isSuccess
  def validateDouble(a: String): Boolean = Try(a.toDouble).isSuccess
  def validateBoolean(a: String): Boolean = Try(a.toBoolean).isSuccess
  def validateDate(a: String): Boolean = Try(Date.valueOf(a)).isSuccess

  def validateDecimal(a: String) = ??? //Try(BigDecimal(a)).isSuccess
  def validateStruct(a: String) = ???
  def validateMap(a: String) = ??? //Try(a.toMap).isSuccess
  def validateArray(a: String) = ??? //Try(a.toArray).isSuccess
  def validateBinary(a: String) = ??? //Try(a.toArray[Byte]).isSuccess

  /**
    * get an instance of org.apche.spark.sql.SaveMode.{Append or Overwrite} matching string
    *
    * @param saveModeStr: String Has a value of Append or Overwrite,specified in param/config file of each spark job,indicating
    *                       whether to append or overwrite data into final target table.
    * @return SaveMode: an instance of org.apche.spark.sql.SaveMode.{Append or Overwrite} matching string
    */
  def getDataLoadStrategy(saveModeStr: String): SaveMode = {
    saveModeStr match {
      case append if (append.equalsIgnoreCase("append")) => SaveMode.Append
      case overwrite if (overwrite.equalsIgnoreCase("overwrite")) =>
        SaveMode.Overwrite
      case _ => throw new Exception("Unsupported SaveMode= " + saveModeStr)
    }
  }

  val getFileNameFromPathUDF: UserDefinedFunction = udf[String, String](_.split("/").last.split('.').head)

}
