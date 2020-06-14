package com.gability.scala.common.io

import java.io.{File, FileInputStream, FileNotFoundException, FileOutputStream, InputStream, IOException}
import java.nio.file.{Files, Paths}
import java.util.zip.ZipInputStream

import scala.io.Source
import scala.util.Try

object FilesHandler {

  /** Check if file exists
    *
    * @param filePath :String file name
    * @return Boolean `true` if file exists `false` if not exists
    */
  def checkFileExists(filePath: String): Boolean = {
    Files.exists(Paths.get(filePath))
  }

  /** read file content from path
    *
    * @param filePath : String
    * @return `Iterator[String]` Iterator of lines represent the file internal content lines
    */
  def readFileFromResources(filePath: String): Iterator[String] = {
    val fileStream: InputStream = getClass.getResourceAsStream(filePath)
    Source.fromInputStream(fileStream).getLines
  }

  //TODO: add function documentation header
  def getFilePath(path: String): String = {
    Try {
      getClass.getResource(path).getPath
    }.getOrElse(
      throw new IllegalArgumentException("Path not found Exception, Path= %s".format(path)) //TODO: create exception types
    )
    /*catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Got an IOException!")
    }*/

  }

  //TODO: add function documentation
  //TODO: add error handler
  def getResourceTestPath(filePath: String): String = {
    getClass.getClassLoader.getResource(filePath).getPath
  }

  /** read file content from path
    *
    * @param filePath : String
    * @return `Try[Iterator[String]]` Iterator of lines if it empty it needs to be applied in matching
    */
  def readResourceFile(filePath: String): Try[String] =
    Try {
      val jsonFile = new File(getResourceTestPath(filePath))
      Source.fromFile(jsonFile).getLines().mkString
    }

  /** getListOfFiles given input and extension pattern filter
    *
    * @param dir        : String Input path
    * @param extension  : String extension for filter
    * @param filesLimit :Int number of files to get from the list
    * @return List[File] list of input path dir filtered by the extension
    */
  def getListOfFiles(dir: String, extension: String, filesLimit: Int): List[File] = {
    val allFilesList = new File(dir).listFiles
      .filter(_.isFile)
      .toList
      .filter(_.getName.endsWith(extension))
    if (filesLimit != -1) {
      allFilesList.take(filesLimit)
    } else {
      allFilesList
    }
  }

  /** unzip the *.zip file and write output to destination path
    *
    * @param inputZipFilePath : String
    * @param destination      : String
    */
  def unzip(inputZipFilePath: String, destination: String): Unit = {
    //val outPutFolder: File = new File(inputZipFilePath)
    val inputFileStream = new FileInputStream(inputZipFilePath)
    val zippedInputStream = new ZipInputStream(inputFileStream)

    Stream
      .continually(zippedInputStream.getNextEntry)
      .takeWhile(_ != null)
      .foreach { file =>
        if (!file.isDirectory) {
          val outPath = Paths.get(destination).resolve(file.getName)
          val outPathParent = outPath.getParent
          if (!outPathParent.toFile.exists()) {
            outPathParent.toFile.mkdirs()
          }

          val outFile = outPath.toFile
          val out = new FileOutputStream(outFile)
          val buffer = new Array[Byte](4096)
          Stream
            .continually(zippedInputStream.read(buffer))
            .takeWhile(_ != -1)
            .foreach(out.write(buffer, 0, _))
        }
      }
  }

}
