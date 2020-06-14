package com.gability.scala.common.metadata

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

object Metadata {

  final case class JobConfig(configDS: Dataset[ConfigParam], sparkSession: SparkSession)

  final case class ConfigParam(jobID: Long, batchId: Long, jobParams: Map[String, String])

  final case class JobsParamConfig(job_id: String,
                                   job_name: String,
                                   config_type: String, //TODO: replace string with struct idea
                                   config_name: String,
                                   config_value: String,
                                   config_seq: String,
                                   update_ts: Timestamp)

  case class JobParamRawDtl(sourceName: String,
                            inputFilesType: String, //TODO: file types enum ```IOType```
                            dataFileDelimiter: String, //TODO: make it Option[String]
                            totalInputFileColumns: String,
                            inputSourcePath: String,
                            rejectedRecordsPath: String,
                            rejectOutputType: String, //TODO: file types enum ```IOType```
                            targetTables: List[String],
                            saveMode: String,
                            processingSuffix: String,
                            outputFormat: String,
                            header: String,
                            partitionColumns: String,
                            inputSchema: List[SchemaDtl])

  final case class SchemaDtl(columnName: String, columnType: String, isNullable: Boolean)

}
