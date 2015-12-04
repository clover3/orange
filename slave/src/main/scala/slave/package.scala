
import java.net._
import java.io._

import common.typedef._
import common.future._
import slave.merger.{SingleThreadMerger, ChunkMerger}
import slave.sorter.SlaveSorter
import slave.Sampler._
import slave.socket._

import scala.concurrent.{Promise, Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}



package object slave {

  class Slave (val master : String, val inputDirs : List[String], val outputDir : String, val tempDir : String)  {
    lazy val socket = new PartitionSocket(master)
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
    def shuffle(sortedFile : List[Future[IBigFile]], partitions : Partitions) : List[IBigFile] = {
      val ipList = partitions.map{_._1}
      val myIp: String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
      val recvList = partitions.map{_._1}.filter(_ != myIp)
      val slaveSock = newShuffleSock(partitions, tempDir, socket)

      def splitFile(file : IBigFile) : List[(String, Int, Int)] = {
        val intList = Splitter.makePartitionsList(file, partitions)
        val result = ipList zip intList
        def f (t:(String,(Int,Int))) = (t._1, t._2._1, t._2._2)
        result map f
      }
      def splitAndSend(sortedFile : List[Future[IBigFile]]) : Future[List[Unit]] = {
        val fileLen = sortedFile.size
        val sendIpList = ipList.filter { _ != myIp}
        sendIpList foreach { slaveSock.sendSize(_, fileLen) }
        def sendAfterSplit(futureFile : Future[IBigFile]) : Future[Unit] = {
          val p = Promise[Unit]()
          def sendFile( t: Try[IBigFile]) = t match {
            case Success(file) =>
              val splitList : List[(String, Int, Int)] = splitFile(file).filter{ _._1 != myIp}
              splitList foreach  {data => slaveSock.sendData(data._1, file, data._2, data._3)}
              println("send data")
              p.complete(Success())
            case Failure(e) => println("I'm in fail " + e.getMessage)
          }

          futureFile onComplete sendFile
          p.future
        }
        all(sortedFile map sendAfterSplit)
      }
      def recv(recvList : List[String]) : List[IBigFile] = {
        val files : List[BigOutputFile] = (recvList map (ip => slaveSock.recvData(ip))).flatten
        files.map( f => f.toInputFile )
      }
      def end(slaveSock : newShuffleSock,f:Future[List[Unit]] ) = {
        f onComplete { t => slaveSock.death() }
      }
      def extractMyPart(sortedFile : List[Future[IBigFile]]) : List[IBigFile] = {
        ???
      }

      val sendFuture = splitAndSend(sortedFile)
      val recvFuture = Future{ recv(recvList) }
      recvFuture onComplete { _ => end(slaveSock, sendFuture) }
      val recvFile = Await.result(recvFuture, Duration.Inf)
      recvFile
    }

    def merge(fileList : List[IBigFile]) : List[String] = {
      val merger : ChunkMerger = new SingleThreadMerger(outputDir)
      merger.MergeBigChunk(fileList)
    }
    def reportResult(fileList: List[String]) = {
      fileList.foreach(x => println(x))
    }

    def testsorted = {
      val d = new File(tempDir)
      d.listFiles.filter(_.isFile).toList.map {
          case f => new SingleFile(tempDir + "/" + f.getName)
      }
    }

    def run() = {
      val partitions     : Partitions                   = getPartition
      val sortedFile     : List[Future[IBigFile]]       = sort
      val netSortedFiles : List[IBigFile]               = shuffle(sortedFile, partitions)
      val sortedResult   : List[String]                 = merge(netSortedFiles)
      val unit                                          = reportResult(sortedResult)
//      val testsortedFile : List[IBigFile]               = testsorted
//      val sortedResult   : IBigFile                     = merge(netSortedFiles)
    }
  }
}

