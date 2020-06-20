package com.gability.scala.common.utils

import com.gability.scala.common.io.FilesHandler._
import pureconfig._
import scala.util.{Failure, Success}

object EnvConfig {

  def parseEnvConfig[T: ConfigReader](envName: String): T = {
    val fileName = "env/" + envName + ".conf"

    val fileConfigData: T = readResourceFile(fileName) match {
      case Success(confStr) => ConfigSource.string(confStr).load[T].right.get
      case Failure(exc)     => throw new IllegalArgumentException(exc)
    }
    fileConfigData
  }

}
