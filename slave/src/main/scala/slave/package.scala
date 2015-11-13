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
import scala.util.{Success, Failure}



package object slave {

  class Slave (val master : String, val inputDirs : List[String], val outputDir : String, val tempDir : String)  {
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
      slaveSorter.run(inputDirs, tempDir)
    }

    def splitAndSend(sortedFile : List[Future[IBigFile]], partitions: Partitions, slaveSock : newShuffleSock) : (List[String],Future[List[Unit]]) = {
      val ipList = partitions.map{_._1}
      val myIp: String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
      def splitFile(file : IBigFile, partitions: Partitions) : List[(String, Int, Int)] = {
        val intList = Splitter.makePartitionsList(file, partitions)
        val result = ipList zip intList
        def f (t:(String,(Int,Int))) = (t._1, t._2._1, t._2._2)
        result map f
      }
      println("===========            splitAndSend          ================")
      println("sortedFile : " + sortedFile)

      val fileLen = sortedFile.size
      val sendIpList = ipList.filter { _ != myIp}
      var sendData = 0;
      println("===========            sendIpList map       ================")
      println("sendIpList : " + sendIpList)
      sendIpList map { ip => slaveSock.sendSize(ip, fileLen) }
      println("===========            sendIpList map end   ================")
      val flist = sortedFile map  {
        futureFile => {
          val p = Promise[Unit]()
          futureFile onComplete {
            case Success(file) =>
              val splitList : List[(String, Int, Int)] = splitFile(file, partitions).filter{ _._1 != myIp}
              println("splitList : " + splitList)
              splitList map {data =>
                  slaveSock.sendData(data._1, file, data._2, data._3);
                  sendData += 1
                  println("I send data" + sendData)
              }
              p.complete(Success())
            case Failure(e) => {
              println("I'm in fail" + e.getMessage)
            }
          }
          p.future
        }
      }
      println("flist : " + flist)
      val l2 = ipList.filter(_ != myIp)
      (l2,all(flist))
    }

    def shuffle(slaveSock : newShuffleSock, recvList : List[String]) : List[IBigFile] = {

      
      val files : List[BigOutputFile] = (recvList map (ip => slaveSock.recvData(ip))).flatten
      slaveSock.death()
      files.map( f => f.toInputFile )
    }
    def end(slaveSock : newShuffleSock,f:Future[List[Unit]] ) = {
        println("############# the end #################")
        Await.result(f, Duration.Inf)
        println("############# the end #################")
    }

    def run() = {
      val partitions     : Partitions                   = getPartition
      val slaveSock      : newShuffleSock               = newShuffleSock(partitions)
      val sortedFile     : List[Future[IBigFile]]       = sort
      //val sortedFile : IBigFile = new ConstFile

      val (recvList,f)  : (List[String],Future[List[Unit]])       = splitAndSend(sortedFile, partitions, slaveSock)
      end(slaveSock,f)
      val netSortedFiles : List[IBigFile]               = shuffle(slaveSock, recvList)
    }
  }

}

