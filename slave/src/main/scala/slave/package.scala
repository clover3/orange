
import java.net._

import common.typedef._
import slave.merger.{SingleThreadMerger, ChunkMerger}
import slave.sorter.SlaveSorter
import slave.future._
import slave.Sampler._
import slave.socket._

import scala.concurrent.{Promise, Future, Await}
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
    def splitAndSend(sortedFile : List[Future[IBigFile]], partitions: Partitions, slaveSock : newShuffleSock) : Future[List[Unit]] = {
      val ipList = partitions.map{_._1}
      val myIp: String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
      def splitFile(file : IBigFile, partitions: Partitions) : List[(String, Int, Int)] = {
        val intList = Splitter.makePartitionsList(file, partitions)
        val result = ipList zip intList
        def f (t:(String,(Int,Int))) = (t._1, t._2._1, t._2._2)
        result map f
      }
      val fileLen = sortedFile.size
      val sendIpList = ipList.filter { _ != myIp}
      sendIpList foreach { slaveSock.sendSize(_, fileLen) }
      def sendAfterSplit(futureFile : Future[IBigFile]) : Future[Unit] = {
        val p = Promise[Unit]()
        futureFile onComplete {
          case Success(file) =>
            val splitList : List[(String, Int, Int)] = splitFile(file, partitions).filter{ _._1 != myIp}
            splitList foreach  {data => slaveSock.sendData(data._1, file, data._2, data._3)}
            println("send data")
            p.complete(Success())
          case Failure(e) => println("I'm in fail " + e.getMessage)
        }
        p.future
      }
      all(sortedFile map sendAfterSplit)
    }
    def recv(slaveSock : newShuffleSock, recvList : List[String], listFuture : Future[List[Unit]]) : List[IBigFile] = {
      val files : List[BigOutputFile] = (recvList map (ip => slaveSock.recvData(ip))).flatten
      end(slaveSock, listFuture)
      files.map( f => f.toInputFile )
    }
    def shuffle(sortedFile : List[Future[IBigFile]], partitions : Partitions, slaveSock : newShuffleSock) : List[IBigFile] = {
      val myIp: String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
      val recvList = partitions.map{_._1}.filter(_ != myIp)
      val listFuture = splitAndSend(sortedFile, partitions, slaveSock)
      recv(slaveSock, recvList, listFuture)
    }
    def end(slaveSock : newShuffleSock,f:Future[List[Unit]] ) = {
      Await.result(f, Duration.Inf)
      slaveSock.death()
    }

    def merge(fileList : List[IBigFile]) : IBigFile = {
      val merger : ChunkMerger = new SingleThreadMerger()
      merger.MergeBigChunk(fileList)
    }

    def run() = {
      val partitions     : Partitions                   = getPartition
      val slaveSock      : newShuffleSock               = newShuffleSock(partitions, tempDir)
      val sortedFile     : List[Future[IBigFile]]       = sort
      val netSortedFiles : List[IBigFile]               = shuffle(sortedFile, partitions, slaveSock)
      val sortedResult   : IBigFile                     = merge(netSortedFiles)
    }
  }
}

