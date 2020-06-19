package com.gability.scala

import com.gability.scala.common.io.FilesHandler._
import pureconfig._
import pureconfig.generic.auto._
import scala.util.{Failure, Success}
object EnvironmentConfig {
  case class Conf(configParam: String, imsiMaster: String)

  def parseEnvConfig(envName: String): Conf = {
    val fileName = "env/" + envName + ".conf"

    val fileConfigData: Conf = readResourceFile(fileName) match {
      case Success(confStr) => ConfigSource.string(confStr).load[Conf].right.get
      case Failure(exc)     => throw new IllegalArgumentException(exc)
    }
    fileConfigData
  }

}
