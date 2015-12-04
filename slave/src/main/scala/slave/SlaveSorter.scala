package slave

import common.future._

import slave.Record._
import slave.util.profile
import slave.SlaveConfig._

import scala.annotation.tailrec
import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, Await, Future}
import scala.math._
import scala.sys.process._

import java.io._

import scala.util.Success


/**
 * Created by Clover on 2015-11-01.
 */

package object sorter {

  // Phase 1 of the sorting process
  trait ChunkSorter {
    def generateSortedChunks(inputs: List[IBigFile]): List[Future[IBigFile]]

    //  TODO :[Optimization] change sort to in-memory sort.
    def sort(data: Vector[BRecord]) : Vector[BRecord] = data.sortBy(rec => rec._1)
    // TODO : as seq and arr both exists, this might results in memory overuse
    def merge(seq: Seq[Vector[BRecord]]) : Vector[BRecord] = {

      val arr : ArrayBuffer[ArrayBuffer[BRecord]] = ArrayBuffer.empty
      seq.foreach( v => {
        val arrElem : ArrayBuffer[BRecord] = ArrayBuffer.empty
        arrElem ++= v
        arr += arrElem
      })

      def findMin(arr:ArrayBuffer[ArrayBuffer[BRecord]]) : (BRecord,Int) = {
        def getMin( t1:(BRecord, Int), t2:(BRecord,Int) ) : (BRecord,Int) = {
          if( t1._1.key > t2._1.key) t2
          else t1
        }
        val firstVal:((BRecord,Int),Int) = ((arr.head.head, 0),0)
        arr.foldLeft(firstVal) {
          (t,vect) => {
            val rec_t = t._1
            val index = t._2
            (getMin(rec_t, (vect.head,index)), index+1)
          }
        }._1
      }
      def removeRec(arr: ArrayBuffer[ArrayBuffer[BRecord]], rec:(BRecord,Int)) : ArrayBuffer[ArrayBuffer[BRecord]] = {
        arr(rec._2) -= rec._1
        if (arr(rec._2).isEmpty)
          arr -= arr(rec._2)
        arr
      }

      @tailrec
      def mergeAcc(sorted:ArrayBuffer[BRecord], arr: ArrayBuffer[ArrayBuffer[BRecord]]) : ArrayBuffer[BRecord] = {
        if( arr.isEmpty )
          sorted
        else {
          val min: (BRecord, Int) = findMin(arr)
          mergeAcc((sorted += min._1), removeRec(arr, min))
        }
      }
      mergeAcc(ArrayBuffer.empty, arr).toVector
    }
  }

  // continueWith

  // DivideChunk: (IBigFile,blockSize) -> (IBigFile,st,ed)
  // read_sort_write : (IBigFile, outfileName, st,ed) -> IBigFile
  // sort : Vector[BRecord] -> [Vector[BRecord]]
  //
  class SingleThreadSorter(val rs: ResourceChecker, val tempDir : String) extends ChunkSorter {
    def divideChunk(inFile:IBigFile, blockSize:Int, outPrefix:String) : IndexedSeq[(IBigFile, String, Int, Int)] = {
      val nTotal = inFile.numOfRecords
      val nBlocks = (nTotal + blockSize - 1) / blockSize
      for {
        i <- Range(0, nBlocks)
      } yield {
        val st = i * blockSize
        val ed = min((i+1) * blockSize, nTotal)
        val outfileName = tempDir+ "/" + outPrefix+i
        (inFile, outfileName, st, ed)
      }
    }



    //inFile:IBigFile, outfileName:String, st:Int, ed:Int
    def read_sort_write( tuple: (IBigFile, String, Int, Int)) : IBigFile = tuple match {
      case (inFile, outfileName, st, ed) => {
        def read = inFile.getRecords(st,ed)
        def write(data: Vector[BRecord]) = {
          val out = new BigOutputFile(tuple._2)
          Await.result(out.setRecords(data), Duration.Inf)
          out
        }
        val f = async {
          val (dataUnsorted,t1)   = profile{ read }
          val (data,t2)           = profile{ sort(dataUnsorted) }
          val (out,t3)            = profile{ write(data) }
          println("Completed Sorting %s : Elapsed %d".format(outfileName, t1+t2+t3))
          out.toInputFile
        }
        Await.result(f, Duration.Inf)
      }
    }

    def schedule2(source : List[IndexedSeq[(IBigFile, String, Int, Int)]]): List[(IBigFile, String, Int, Int)] = {
      source.flatten
    }

    def schedule(source : List[IndexedSeq[(IBigFile, String, Int, Int)]]): List[(IBigFile, String, Int, Int)] = {
      val headList = source.flatMap(l => l.headOption)
      val tailSource = source.flatMap(l => if(l.tail == Nil) None else Option(l.tail))
      val tailList = if( tailSource == Nil ) Nil else schedule(tailSource)
      headList ::: tailList
    }

    def generateSortTasks(files:List[IBigFile])(implicit config:Config) = {
      val mem = rs.remainingMemory
      val blockSize = config.sortBlockSize
      println("remaining memory :"+mem + " blockSize:"+ blockSize)
      println("Supporterable threads : " + mem / ( blockSize * 100 * 2))

      val namePrefix = "sortedChunk"
      val names = for( i <- Range(0,files.size)) yield { namePrefix+ i + "-"}
      val inputs = files.zip(names)
      inputs.map( input => divideChunk(input._1, blockSize, input._2))
    }

    def generateSortedChunks(files: List[IBigFile]): List[Future[IBigFile]] = {
      val tasks:List[IndexedSeq[(IBigFile, String, Int, Int)]] = generateSortTasks(files)
      val scheduledTask: List[(IBigFile, String, Int, Int)] = schedule(tasks)
      val completedTasks = scheduledTask.map(t => read_sort_write(t))
      completedTasks.map( t=> Future{t}).toList
    }
  }

  class MultiThreadSorter(val rs2: ResourceChecker, override val tempDir : String) extends SingleThreadSorter(rs2, tempDir){
    override
    def generateSortedChunks(files: List[IBigFile]): List[Future[IBigFile]] = {
      val tasks:List[IndexedSeq[(IBigFile, String, Int, Int)]] = generateSortTasks(files)
      val tasksScheduled = schedule(tasks)
      tasksScheduled.map( t => Future{read_sort_write(t)} ).toList
    }
  }
/*
  class CPPSorter (val tempDIr :String){
    def generateSortedChunks(files : List[String]) : List[Future[IBigFile]] = {
      val size = files.size
      val promiseList : List[Promise[IBigFile]] = for(i<-size) yield {Promise[IBigFile]()}
      taskRunner(files, nameList(size), promiseList)
      promiseList.map(p => p.future)
    }
    def nameList(size:Int) : List[String] = {
      val namePrefix = "sortedChunk"
      val names = for( i <- Range(0,size)) yield { namePrefix+ i + "-"}
      names.toList
    }
    def taskRunner(files: List[String], outname: List[String], promiseList: List[Promise[IBigFile]])
    : Future[Unit] = Future{
      val tasks = (files zip outname zip promiseList) map (t => (t._1._1, t._1._2, t._2))
      tasks map (t => read_sort_write(t._1, t._2, t._3))
    }

    def read_sort_write(path : String, outpath :String, p : Promise[IBigFile]) : Future[Unit] = Future{
      val cmd = "./a.exe " + path + " " + outpath
      cmd.!!
      val file:IBigFile = new SingleFile(outpath)
      p.complete(Success(file))
    }
  }
*/

  // sortChunk( IBigFile, String, Int, Int) => IBigFile
  // -> sortMiniChunk : (IBigFile, Int, Int) => Future[Vector[Record]]
  // -> mergeMiniChunk : (Future[List[Vector[BRecord]]]) => Vector[BRecord]
  class MultiThreadMergeSorter(val rs2: ResourceChecker, override val tempDir : String) extends SingleThreadSorter(rs2, tempDir){
    def divideInterval(n:Int, st:Int, ed:Int) : List[(Int,Int)] = {
      assert( ed - st > n )
      val size = (ed - st + n - 1) / n
      (for {
        i <- Range(0, n)
      } yield {
        val from = i * size
        val to = min((i+1) * size, ed)
        (from, to)
      }).toList
    }

    def read_sort(file: IBigFile, st: Int, ed: Int): Future[Vector[BRecord]] = async{
      val data : Vector[BRecord] = file.getRecords(st,ed)
      sort(data)
    }

    def sortMiniChunk(input: IBigFile, st:Int, ed:Int) : Future[Vector[BRecord]] = {
      read_sort(input, st, ed)
    }


    def mergeMiniChunk(fList: Future[Seq[Vector[BRecord]]]) : Future[Vector[BRecord]] = async {
      val seq : Seq[Vector[BRecord]] = await{ fList }
      merge(seq)
    }

    def sortChunk( tuple: (IBigFile, String, Int, Int)) : IBigFile = tuple match {
      case (input, outfileName, st, ed) => {
        val numCores = 4
        val intervals  = divideInterval(numCores, st, ed)
        val futureChunks : List[Future[Vector[BRecord]]] = intervals.map { tuple:(Int,Int) =>
          sortMiniChunk(input, tuple._1, tuple._2)
        }
        val futureSortedChunk : Future[Vector[BRecord]] = mergeMiniChunk(all(futureChunks))
        val sortedBigChunk : Vector[BRecord] = Await.result(futureSortedChunk, Duration.Inf)
        val outfile: IOutputFile = new BigOutputFile(outfileName)
        outfile.setRecords(sortedBigChunk)
        outfile.toInputFile
      }
    }

    override def generateSortedChunks(inputs: List[IBigFile]): List[Future[IBigFile]] = {
      val mem = rs.remainingMemory
      println("Mem : " + mem)
      val blockSize = 327680
      def outPath(implicit config:Config) = config.tempPath
      val tasks = inputs.map(input => divideChunk(input, blockSize, outPath))
      val tasksScheduled = schedule(tasks)
      tasksScheduled.map( t => Future{sortChunk(t)} ).toList
    }
  }


  class SlaveSorter {
    def run(inputDirs: List[String], tempDir : String): List[Future[IBigFile]] = {
      // initializing structs
      val input: List[IBigFile] = inputDirs.map(d => new MultiFile(List(d)))
      //val input : IBigFile = new ConstFile
      val rs:ResourceChecker = new ResourceChecker()
      val sorttempDir = tempDir + "/" + "sorted"
      val d = new File(sorttempDir)
      if(!d.exists)
        d.mkdir()
      val sorter: ChunkSorter = new MultiThreadSorter(rs, sorttempDir)
      // operate on
      sorter.generateSortedChunks(input)
    }
  }

}
