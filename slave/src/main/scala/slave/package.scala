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

import scala.concurrent.{Promise, Future, Await}
import scala.io._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Success


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

    def sort : List[Future[IBigFile]] = {
      val slaveSorter = new SlaveSorter()
      slaveSorter.run(inputDirs)
    }

    def splitAndSend(sortedFile : List[Future[IBigFile]], partitions: Partitions, slaveSock : newShuffleSock) : (List[String],Future[List[Unit]]) = {
      val ipList = partitions.map{_._1}
      val myIp: String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
      def splitFile(file : IBigFile, partitions: Partitions) : List[(String, Int, Int)] = {
        val intList = Splitter.makePartitionsList(file, partitions)
//        println("intList : " + intList)
        val result = ipList zip intList
//        println("result : " + result)
        def f (t:(String,(Int,Int))) = (t._1, t._2._1, t._2._2)
        result map f
      }
      val fileLen = sortedFile.size
      val sendIpList = ipList.filter { _ != myIp}
      val expectedSendLen = fileLen * sendIpList.size
//      println("sendIpList : " + sendIpList)
//      println("fileLen : " + fileLen)
      sendIpList map { ip => slaveSock.sendSize(ip, fileLen) }
      val flist = sortedFile map  {
        futureFile => {
          val p = Promise[Unit]()
          futureFile onSuccess {
            case file =>
              val splitList : List[(String, Int, Int)] = splitFile(file, partitions).filter{ _._1 != myIp}
              //            println("splitList : " + splitList)
              splitList map {data =>
                  slaveSock.sendData(data._1, file, data._2, data._3);
              }
              p.complete(Success())
          }
          p.future
        }
      }
      val l2 = ipList.filter(_ != myIp)
      (l2,all(flist))
    }

    def shuffle(slaveSock : newShuffleSock, recvList : List[String]) : List[IBigFile] = {

//      print("ipList : ")
      println(recvList)
      // recvList map (ip => Await.result(slaveSock.recvData(ip), Duration.Inf))
      
      val files : List[BigOutputFile] = (recvList map (ip => slaveSock.recvData(ip))).flatten
      //resultList foreach {Await.result(_, Duration.Inf)}
      files.map( f => f.toInputFile )
    }
    def end(slaveSock : newShuffleSock,f:Future[List[Unit]] ) = {
        Await.result(f, Duration.Inf)
    }

    def run() = {
      val partitions     : Partitions                   = getPartition
      val slaveSock      : newShuffleSock               = ShuffleSocket(partitions)
      val sortedFile     : List[Future[IBigFile]]       = sort
      //val sortedFile : IBigFile = new ConstFile

      val (recvList,f)  : (List[String],Future[List[Unit]])       = splitAndSend(sortedFile, partitions, slaveSock)
      val netSortedFiles : List[IBigFile]               = shuffle(slaveSock, recvList)
      end(slaveSock,f)
    }
  }

}

