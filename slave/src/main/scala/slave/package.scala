
import java.net._
import java.io._

import common.typedef._
import common.future._
import slave.merger._
import slave.sorter.SlaveSorter
import slave.Sampler._
import slave.socket._
import slave.util._


import scala.concurrent.{Promise, Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.async.Async.{async, await}


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
        class SendLog(val total:Int){
          val id = "Send"
          var completed = 0
          def getMsg() = "Send ||" + "=" * completed + " " *(total-completed) + "||"
          val unit = updateLog()
          def updateLog() = ProgressLogger.updateLog(id, getMsg())
          def addComplete() = {
            completed = completed + 1
            updateLog()
          }
        }
        val logger = new SendLog(sortedFile.size)

        val fileLen = sortedFile.size
        val sendIpList = ipList.filter { _ != myIp}
        sendIpList foreach { slaveSock.sendSize(_, fileLen) }
        def sendAfterSplit(futureFile : Future[IBigFile]) : Future[Unit] = {
          val p = Promise[Unit]()
          def sendFile( t: Try[IBigFile]) = t match {
            case Success(file) =>
              val splitList : List[(String, Int, Int)] = splitFile(file).filter{ _._1 != myIp}
              splitList foreach  {data => slaveSock.sendData(data._1, file, data._2, data._3)}
              logger.addComplete()
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
      def extractMyPart(sortedFile : List[Future[IBigFile]]) : Future[List[IBigFile]] = async {
        val fileList : List[IBigFile] = await { all(sortedFile) }
        val filePartList= fileList.map(f => (f,splitFile(f).filter(t=>t._1== myIp)) )
        def toPartialFiles(file:IBigFile, parts: List[(String,Int,Int)] ) = {
          parts.map(t => new PartialFile(file, t._2, t._3) )
        }
        val partialFileList = filePartList.map(t => toPartialFiles(t._1, t._2) ).flatten
        partialFileList
      }

      val sendFuture = splitAndSend(sortedFile)
      val recvFuture = Future{ recv(recvList) }
      recvFuture onComplete { _ => end(slaveSock, sendFuture) }

      val myFile = Await.result(extractMyPart(sortedFile), Duration.Inf)
      val recvFile = Await.result(recvFuture, Duration.Inf)

      recvFile:::myFile
    }

    def merge(fileList : List[IBigFile]) : List[String] = {
      val merger : ChunkMerger = new MultiThreadMerger(outputDir)
      merger.MergeBigChunk(fileList)
    }
    def reportResult(fileList: List[String]) = {
      println("Sorted Files :")
      fileList.foreach(x => println(x))
    }

    def testsorted = {
      val d = new File(tempDir)
      d.listFiles.filter(_.isFile).toList.map {
          case f => new SingleFile(tempDir + "/" + f.getName)
      }
    }

    def run() = {
      val (_,time) = profile {
        val partitions: Partitions = getPartition
        val sortedFile: List[Future[IBigFile]] = sort
        val netSortedFiles: List[IBigFile] = shuffle(sortedFile, partitions)
        val sortedResult: List[String] = merge(netSortedFiles)
        val unit = reportResult(sortedResult)
        //      val testsortedFile : List[IBigFile]               = testsorted
        //      val sortedResult   : IBigFile                     = merge(netSortedFiles)
      }
      println("Slave operation completed. Time Elapsed(ms) : " + time)
    }
  }
}

