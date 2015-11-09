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
      // val forwardData : List[(String, IBigFile, Int, Int)] = ???
      val myIp: String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
      print("myIp : ") ; println(myIp)
      val forwardData : List[(String, IBigFile, Int, Int)] = List(("192.168.10.1", sortedFile, 0, 1000000), ("192.168.10.2", sortedFile, 0, 1000000)).filter{_._1 != myIp}
      val ipList = (partitions.map {_._1})
      val slaveSock = ShuffleSocket(ipList)
      print("forwardData : ")
      println(forwardData)
      val resultList = forwardData map (data => slaveSock.sendData(data._1, data._2, data._3, data._4))
      val recvList = ipList.filter{_ != myIp}
      print("ipList : ")
      println(recvList)
      val files :List[BigOutputFile] = Await.result(all(recvList map (ip => slaveSock.recvData(ip))), Duration.Inf)
      //resultList foreach {Await.result(_, Duration.Inf)}
      slaveSock.death()
      files.map( f => f.toInputFile )
    }

    def run() = {
      val partitions     : Partitions     = getPartition
      // val sortedFile     : IBigFile       = sort
      val sortedFile : IBigFile = new ConstFile
      val netSortedFiles : List[IBigFile] = shuffle (partitions, sortedFile)
    }
  }

}

