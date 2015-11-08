import java.io._
import java.net._
import java.nio._
import java.nio.channels._


import org.apache.commons.logging.LogFactory

import common.typedef._
import slave.sorter.SlaveSorter
import slave.future._
import slave.Sampler._
import slave.socket._

import scala.concurrent.Future
import scala.io._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._


package object slave {

  class Slave (val master : String, val inputDirs : List[String], val outputDir : String)  {
    val socket = new PartitionSocket(master)
    var inputDir: List[String] = Nil

    def getPartition : Partitions = {
      val sampler = SlaveSampler(socket, inputDirs, outputDir)
      val partitions = sampler.getPartition
      print("this is partition : "); println (partitions)
      partitions
    }

    def sort : IBigFile = {
      val slaveSorter = new SlaveSorter()
      slaveSorter.run(inputDirs)
    }

    def shuffle(partitions:Partitions, sortedFile: IBigFile) : List[IBigFile] = {
      val forwardData : List[(String, IBigFile, Int, Int)] = ???
      val ipList = partitions map {_._1}
      val slaveSock = ShuffleSocket(ipList)
      forwardData foreach (data => slaveSock.sendData(data._1, data._2, data._3, data._4))
      val files :List[BigOutputFile] = Await.result(all(ipList map (ip => slaveSock.recvData(ip))), Duration.Inf)
      slaveSock.death()
      files.map( f => f.toInputFile )
    }

    def run() = {
      val partitions     : Partitions     = getPartition
      val sortedFile     : IBigFile       = sort
      val netSortedFiles : List[IBigFile] = shuffle (partitions, sortedFile)
    }
  }

}

