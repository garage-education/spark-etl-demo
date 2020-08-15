package com.gability.scala.common.metadata

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

object Metadata {

  final case class JobConfig(configDS: ConfigParam, sparkSession: SparkSession)

  final case class ConfigParam(jobID: Long, batchId: Long, jobParams: Map[String, String])

  final case class JobsParamConfig(job_id:       String,
                                   job_name:     String,
                                   config_type:  String, //TODO: replace string with struct idea
                                   config_name:  String,
                                   config_value: String,
                                   config_seq:   String,
                                   update_ts:    Timestamp)

  case class JobParamRawDtl(sourceName: String, inputSource: InputSource, rejection: Rejection, targetSource: List[TargetSource])

  final case class TargetSource(targetTable:      String,
                                targetSchema:     String,
                                partitionColumns: String,
                                outputFormat:     String,
                                saveMode:         String)

  final case class SchemaDtl(columnName: String, columnType: String, isNullable: Boolean)
  final case class Rejection(
      rejectedRecordsPath: String,
      rejectOutputType:    String //TODO: filtargetSourcee types enum ```IOType```
  )
  final case class InputSource(
      inputFilesType:        String, //TODO: file types enum ```IOType```
      dataFileDelimiter:     String, //TODO: make it Option[String]
      totalInputFileColumns: String,
      inputSourcePath:       String,
      processingSuffix:      String,
      header:                String,
      inputSchema:           List[SchemaDtl]
  )

}
