package slave

import java.io.{FileNotFoundException, File}

import scala.io.Source

package object SlaveConfig {

  trait Config {
    def sortBlockSize: Int
    def numMergeThread: Int
    def tempPath : String
  }

  implicit object Config extends Config {
    def sortBlockSize: Int = valSortBlockSize
    def numMergeThread:Int = valNumMergeThread
    def tempPath :String = valTempPath
    lazy val (valSortBlockSize, valNumMergeThread, valTempPath) = {
      load()
    }
    def load() = {
      val configPath = "config"
      val file = new File(configPath)
      if (file.exists) {
        val fileStream: Stream[String] = Source.fromFile(file).getLines().toStream;
        val configList = fileStream.toVector

        val blockSize = configList(0).toInt
        val numThread = configList(1).toInt
        val tempPath = configList(2)
        println("Config :" )
        println("blockSize="+ blockSize)
        println("numMergeThread="+ numThread)
        println("tempPath="+ tempPath)
        (blockSize, numThread, tempPath)
      }
      else
        throw new FileNotFoundException
    }
  }

}