package slave

import java.io.{FileNotFoundException, File}

import scala.io.Source

package object SlaveConfig {

  trait Config {
    def sortBlockSize: Int
    def numMergeThread: Int
  }

  implicit object Config extends Config {
    def sortBlockSize: Int = valSortBlockSize
    def numMergeThread:Int = valNumMergeThread
    lazy val (valSortBlockSize, valNumMergeThread) = {
      load()
    }
    def load() = {
      val configPath = "config"
      val file = new File(configPath)
      if (file.exists) {
        val fileStream: Stream[String] = Source.fromFile(file).getLines().toStream;
        val configList = fileStream.take(2).toVector

        val blockSize = configList(0).toInt
        val numThread = configList(1).toInt
        println("Config :" )
        println("blockSize="+ blockSize)
        println("numMergeThread="+ numThread)
        (blockSize,numThread)
      }
      else
        throw new FileNotFoundException
    }
  }

}