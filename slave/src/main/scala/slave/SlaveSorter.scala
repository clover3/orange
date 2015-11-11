package slave

import slave.Record._
import slave.future._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.math._

import scala.concurrent.{Await, Promise, ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global



/**
 * Created by Clover on 2015-11-01.
 */

package object sorter {

  val sortedFileName = "sorted"
  // Phase 1 of the sorting process
  trait ChunkSorter {
    def generateSortedChunks(input: IBigFile): List[IBigFile]

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
        arr(rec._2)-=rec._1
        if( arr(rec._2).isEmpty )
          arr -= arr(rec._2)
        arr
      }

      @tailrec
      def mergeAcc(sorted:ArrayBuffer[Record], arr: ArrayBuffer[ArrayBuffer[Record]]) : ArrayBuffer[Record] = {
        if( arr.isEmpty )
          sorted
        val min : (Record,Int) = findMin(arr)
        mergeAcc( (sorted+= min._1), removeRec(arr, min) )
      }
      mergeAcc(ArrayBuffer.empty, arr).toVector
    }
  }

  // Phase 2 of the sorting process
  trait ChunkMerger {
    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile
  }

  // TODO OS might not keep pages of the files, It might be better to read sequence of data
  class SingleThreadMerger extends ChunkMerger {
    //TODO we can optimize by changing idxSeq to mutable
    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile =
    {
      def getHead( file:IBigFile ) : Record = file.getRecord(0)
      val output : BigOutputFile = new BigOutputFile(sortedFileName)

      val nChunks = sortedChunks.size
      val minList :List[Record]= sortedChunks.map(getHead)
      val idxSeq :Seq[Int] = for( i <- Range(0, nChunks) ) yield 0

      val arrChunks: ArrayBuffer[IBigFile] = ArrayBuffer.empty  //
      val minArr   : ArrayBuffer[Record]   = ArrayBuffer.empty
      val idxArr   : ArrayBuffer[Int]      = ArrayBuffer.empty

      arrChunks++= sortedChunks
      minArr++= minList
      idxArr++= idxSeq

      def getMinRec() = minArr.minBy( rec => rec.key )
      def updateMinArray(minRec : Record) = {
        val idxMinChunk:Int = minArr.indexOf(minRec)                        // idxMin represents the selected chunk
        idxArr(idxMinChunk) = idxArr(idxMinChunk) + 1
        if( arrChunks(idxMinChunk).numOfRecords <= idxArr(idxMinChunk) )      //
        {
          minArr.remove(idxMinChunk)
          idxArr.remove(idxMinChunk)
          arrChunks.remove(idxMinChunk)
        }
        else
          minArr(idxMinChunk) = arrChunks(idxMinChunk).getRecord( idxArr(idxMinChunk) )
      }
      @tailrec
      def mergeIteration : Unit = {
        if( minArr.isEmpty )
          return

        val minRec = getMinRec()
        output.appendRecord(minRec)

        updateMinArray(minRec)

        mergeIteration
      }
      mergeIteration
      output.close()
      output.toInputFile
    }
  }


  def getBlockSize(memSize :Int) : Int = {
    100 * 10000
        //memSize / 100 / 2
  }
  // continueWith

  // DivideChunk: (IBigFile,blockSize) -> (IBigFile,st,ed)
  // read_sort_write : (IBigFile, outfileName, st,ed) -> IBigFile
  // sort : Vector[Record] -> [Vector[Record]]
  //
  class SingleThreadSorter(val rs: ResourceChecker) extends ChunkSorter {
    def divideChunk(inFile:IBigFile, blockSize:Int, outPrefix:String) : IndexedSeq[(IBigFile, String, Int, Int)] = {
      val nTotal = inFile.numOfRecords
      val nBlocks = (nTotal + blockSize - 1) / blockSize
      for {
        i <- Range(0, nBlocks)
      } yield {
        val st = i * blockSize
        val ed = min((i+1) * blockSize, nTotal)
        val outfileName = outPrefix+i
        println(outfileName)
        (inFile, outfileName, st, ed)
      }
    }

    //inFile:IBigFile, outfileName:String, st:Int, ed:Int
    def read_sort_write( tuple: (IBigFile, String, Int, Int)) : IBigFile = tuple match {
      case (inFile, outfileName, st, ed) => {
        val f = async {
          val dataUnsorted : Vector[Record] = inFile.getRecords(st,ed)
          val data = sort(dataUnsorted)
          val outfile: IOutputFile = new BigOutputFile(tuple._2)
          await {
            outfile.setRecords(data)
          }
          outfile.toInputFile
        }
        Await.result(f, Duration.Inf)
      }
    }

    def read_sort(file: IBigFile, st: Int, ed: Int): Future[Vector[Record]] = async{
      val data : Vector[Record] = file.getRecords(st,ed) 
      sort(data)
    }

    def generateSortedChunks(input: IBigFile): List[IBigFile] = {
      val mem = rs.remainingMemory
      val blockSize = getBlockSize(mem)
      println("mem :"+mem + " blockSize:"+ blockSize)
      val inputSeq = divideChunk(input, blockSize, "temp/sortedChunk")
      inputSeq.map(read_sort_write).toList
    }
  }

  // sortChunk( IBigFile, String, Int, Int) => IBigFile
  // -> sortMiniChunk : (IBigFile, Int, Int) => Future[Vector[Record]]
  // -> mergeMiniChunk : (Future[List[Vector[Record]]]) => Vector[Record]
  class MultiThreadSorter(val rs2: ResourceChecker) extends SingleThreadSorter(rs2){
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

    override def generateSortedChunks(input: IBigFile): List[IBigFile] = {
      val mem = rs.remainingMemory
      println("Mem : " + mem)
      val blockSize = getBlockSize(mem)
      val inputSeq = divideChunk(input, blockSize, "sortedChunk")
      inputSeq.map(sortChunk).toList
    }
  }


  class SlaveSorter {
    def run(inputDirs: List[String]): IBigFile = {
      // initializing structs
      val input: IBigFile = new MultiFile(inputDirs)
      //val input : IBigFile = new ConstFile
      val rs:ResourceChecker = new ResourceChecker()
      val merger: ChunkMerger = new SingleThreadMerger
      val sorter: ChunkSorter = new SingleThreadSorter(rs)

      // operate on
      val sortedChunks: List[IBigFile] = sorter.generateSortedChunks(input)
      merger.MergeBigChunk(sortedChunks)
    }
  }

}
