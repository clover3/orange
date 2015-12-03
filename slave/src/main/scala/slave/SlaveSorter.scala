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
import scala.concurrent.{Await, Future}
import scala.math._

import java.io._


/**
 * Created by Clover on 2015-11-01.
 */

package object sorter {

  // Phase 1 of the sorting process
  trait ChunkSorter {
    def generateSortedChunks(inputs: List[IBigFile]): List[Future[IBigFile]]

    //  TODO :[Optimization] change sort to in-memory sort.
    def sort(data: Vector[Record]) : Vector[Record] = data.sortBy(rec => rec._1)
    // TODO : as seq and arr both exists, this might results in memory overuse
    def merge(seq: Seq[Vector[Record]]) : Vector[Record] = {

      val arr : ArrayBuffer[ArrayBuffer[Record]] = ArrayBuffer.empty
      seq.foreach( v => {
        val arrElem : ArrayBuffer[Record] = ArrayBuffer.empty
        arrElem ++= v
        arr += arrElem
      })

      def findMin(arr:ArrayBuffer[ArrayBuffer[Record]]) : (Record,Int) = {
        def getMin( t1:(Record, Int), t2:(Record,Int) ) : (Record,Int) = {
          if( t1._1.key > t2._1.key) t2
          else t1
        }
        val firstVal:((Record,Int),Int) = ((arr.head.head, 0),0)
        arr.foldLeft(firstVal) {
          (t,vect) => {
            val rec_t = t._1
            val index = t._2
            (getMin(rec_t, (vect.head,index)), index+1)
          }
        }._1
      }
      def removeRec(arr: ArrayBuffer[ArrayBuffer[Record]], rec:(Record,Int)) : ArrayBuffer[ArrayBuffer[Record]] = {
        arr(rec._2) -= rec._1
        if (arr(rec._2).isEmpty)
          arr -= arr(rec._2)
        arr
      }

      @tailrec
      def mergeAcc(sorted:ArrayBuffer[Record], arr: ArrayBuffer[ArrayBuffer[Record]]) : ArrayBuffer[Record] = {
        if( arr.isEmpty )
          sorted
        else {
          val min: (Record, Int) = findMin(arr)
          mergeAcc((sorted += min._1), removeRec(arr, min))
        }
      }
      mergeAcc(ArrayBuffer.empty, arr).toVector
    }
  }

  // continueWith

  // DivideChunk: (IBigFile,blockSize) -> (IBigFile,st,ed)
  // read_sort_write : (IBigFile, outfileName, st,ed) -> IBigFile
  // sort : Vector[Record] -> [Vector[Record]]
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
        def write(data: Vector[Record]) = {
          val out = new BigOutputFile(tuple._2)
          Await.result(out.setRecords(data), Duration.Inf)
          out
        }
        val f = async {
          val (dataUnsorted,t1)   = profile{ read }
          val (data,t2)           = profile{ sort(dataUnsorted) }
          val (out,t3)            = profile{ write(data) }
          println("Sort %s read/sort/write=(%d/%d/%d)".format(outfileName, t1,t2,t3))
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

  // sortChunk( IBigFile, String, Int, Int) => IBigFile
  // -> sortMiniChunk : (IBigFile, Int, Int) => Future[Vector[Record]]
  // -> mergeMiniChunk : (Future[List[Vector[Record]]]) => Vector[Record]
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

    def read_sort(file: IBigFile, st: Int, ed: Int): Future[Vector[Record]] = async{
      val data : Vector[Record] = file.getRecords(st,ed)
      sort(data)
    }

    def sortMiniChunk(input: IBigFile, st:Int, ed:Int) : Future[Vector[Record]] = {
      read_sort(input, st, ed)
    }


    def mergeMiniChunk(fList: Future[Seq[Vector[Record]]]) : Future[Vector[Record]] = async {
      val seq : Seq[Vector[Record]] = await{ fList }
      merge(seq)
    }

    def sortChunk( tuple: (IBigFile, String, Int, Int)) : IBigFile = tuple match {
      case (input, outfileName, st, ed) => {
        val numCores = 4
        val intervals  = divideInterval(numCores, st, ed)
        val futureChunks : List[Future[Vector[Record]]] = intervals.map { tuple:(Int,Int) =>
          sortMiniChunk(input, tuple._1, tuple._2)
        }
        val futureSortedChunk : Future[Vector[Record]] = mergeMiniChunk(all(futureChunks))
        val sortedBigChunk : Vector[Record] = Await.result(futureSortedChunk, Duration.Inf)
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
      val sorter: ChunkSorter = new MultiThreadSorter(rs, tempDir)
      // operate on
      sorter.generateSortedChunks(input)
    }
  }

}
