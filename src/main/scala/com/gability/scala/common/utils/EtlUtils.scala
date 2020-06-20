package com.gability.scala.common.utils

import java.sql.{Date, Timestamp}

import com.gability.scala.common.metadata.Metadata.JobParamRawDtl
import com.gability.scala.common.utils.JsonExtractor._
import com.gability.scala.common.utils.TypeValidator._
import org.apache.spark.sql.{Dataset, Encoders, Row, SaveMode}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
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

  def validateDataset(inputDs: Dataset[Row], schemaStruct: StructType): (Dataset[Row], Dataset[Row]) = {

    val colNames = schemaStruct.map(_.name)
    val inputDsRenamed = inputDs.toDF(colNames: _*)

    //TODO: reduce the dataframe scan one idea is to add a new column with match Boolean flag
    val validDf = inputDsRenamed
      .filter(schemaParser(_, structSchemaValidator(schemaStruct)))

    //TODO: add rejection reason
    val inValidDf = inputDsRenamed
      .filter(!schemaParser(_, structSchemaValidator(schemaStruct)))

    (validDf, inValidDf)
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
        nullableValidator(validatorMap(x.dataType))
      } else
        validatorMap(x.dataType)
    }

  val validatorMap: Map[DataType, String => Boolean] = Map(
    StringType    -> canParse[String],
    IntegerType   -> canParse[Int],
    TimestampType -> canParse[Timestamp],
    ShortType     -> canParse[Short],
    LongType      -> canParse[Long],
    FloatType     -> canParse[Float],
    DoubleType    -> canParse[Double],
    BooleanType   -> canParse[Boolean],
    DateType      -> canParse[Date]
  )

  def nullableValidator(validator: String => Boolean): String => Boolean = a => a == null || validator(a)

  def schema2CaseClassValidator[T <: Product: ru.TypeTag](): Seq[String => Boolean] = {
    val personEncoder = Encoders.product[T]
    val personSchema: StructType = personEncoder.schema
    structSchemaValidator(personSchema)
  }

  /**
    * get an instance of org.apche.spark.sql.SaveMode.{Append or Overwrite} matching string
    *
    * @param saveModeStr: String Has a value of Append or Overwrite,specified in param/config file of each spark job,indicating
    *                       whether to append or overwrite data into final target table.
    * @return SaveMode: an instance of org.apche.spark.sql.SaveMode.{Append or Overwrite} matching string
    */
  def getDataLoadStrategy(saveModeStr: String): SaveMode = {
    saveModeStr match {
      case append if (append.equalsIgnoreCase("append"))          => SaveMode.Append
      case overwrite if (overwrite.equalsIgnoreCase("overwrite")) => SaveMode.Overwrite
      case _                                                      => throw new Exception("Unsupported SaveMode= " + saveModeStr)
    }
  }

}
