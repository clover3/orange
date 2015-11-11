package slave

import slave.Record._
import slave.future._
import slave.util.profile

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.math._

import scala.concurrent.{Await, Promise, ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Sorting


/**
 * Created by Clover on 2015-11-01.
 */

package object sorter {

  val sortedFileName = "sorted"
  // Phase 1 of the sorting process
  trait ChunkSorter {
    def generateSortedChunks(input: IBigFile): List[Future[IBigFile]]

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

  // Phase 2 of the sorting process
  trait ChunkMerger {
    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile
  }

  // TODO OS might not keep pages of the files, It might be better to read sequence of data
  class SingleThreadMerger extends ChunkMerger {
    //TODO we can optimize by changing idxSeq to mutable
    def MergeWithPQ(sortedChunks :List[IBigFile]): IBigFile = {
      val output : BigOutputFile = new BigOutputFile(sortedFileName)

      class DataBag(sources: Vector[IBigFile] ) {
        val cursorArray : ArrayBuffer[Int] = ArrayBuffer.empty ++ (for( i <- Range(0, sources.size) ) yield 0)
        def getItem(index:Int) : Option[Record] = {
          val cursor = cursorArray(index)
          if( cursor < sources(index).numOfRecords ) {
            val rec = sources(index).getRecord(cursor)
            cursorArray(index) = cursorArray(index)+1
            Some(rec)
          }
          else
            None
        }
        def isEmpty : Boolean = {
          Range(0,sources.size).forall( i => {
            sources(i).numOfRecords <= cursorArray(i)
          })
        }
      }

      class MergePQ(sortedChunks :List[IBigFile]) {
        object EntryOrdering extends Ordering[(Record,Int)] {
          def compare(a:(Record,Int), b:(Record,Int)) = b._1._1 compare a._1._1
        }

        val priorityQueue : mutable.PriorityQueue[(Record,Int)] = new mutable.PriorityQueue[(Record,Int)]()(EntryOrdering)
        val dataBag = new DataBag(sortedChunks.toVector)
        val contructor = {
          println("Contructor Executed")
          for( i <- Range(0, sortedChunks.size) ) {
            val rec: Option[Record] = dataBag.getItem(i)
            rec match {
              case Some(r) => priorityQueue.enqueue((r,i))
              case None => ()
            }
          }
        }
        def getMin() : Record = {
          val (rec,i) = priorityQueue.dequeue()
          dataBag.getItem(i) match {
            case Some(r) => (priorityQueue+=((r,i)))
            case None => ()
          }
          rec
        }
        def isEmpty : Boolean = priorityQueue.isEmpty
      }

      val pq : MergePQ = new MergePQ(sortedChunks)
      while( !pq.isEmpty )
      {
        output.appendRecord(pq.getMin())
      }
      output.close()
      output.toInputFile
    }

    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile = {
//      MergeWithPQ(sortedChunks)
      MergeSimple(sortedChunks)
    }

    def MergeSimple(sortedChunks: List[IBigFile]): IBigFile =
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
    //50 * 10000
    println("memSize:" + memSize)
    //5000
    100000
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

    def generateSortedChunks(input: IBigFile): List[Future[IBigFile]] = {
      val mem = rs.remainingMemory
      val blockSize = getBlockSize(mem)
      println("mem :"+mem + " blockSize:"+ blockSize)
      val inputSeq = divideChunk(input, blockSize, "temp/sortedChunk")
      inputSeq.map(t => Future{read_sort_write(t)}).toList
    }
  }

  class MultiThreadSorter(val rs2: ResourceChecker) extends SingleThreadSorter(rs2){
    override
    def generateSortedChunks(input: IBigFile): List[Future[IBigFile]] = {
      val mem = rs.remainingMemory
      val blockSize = getBlockSize(mem / 4)
      println("mem :"+mem + " blockSize:"+ blockSize)
      val inputSeq = divideChunk(input, blockSize, "sortedChunk")
      val lstFutureFile : List[Future[IBigFile]]= {
        inputSeq.map( t => Future{read_sort_write(t)}).toList
      }
      lstFutureFile
    }
  }

  // sortChunk( IBigFile, String, Int, Int) => IBigFile
  // -> sortMiniChunk : (IBigFile, Int, Int) => Future[Vector[Record]]
  // -> mergeMiniChunk : (Future[List[Vector[Record]]]) => Vector[Record]
  class MultiThreadMergeSorter(val rs2: ResourceChecker) extends SingleThreadSorter(rs2){
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

    override def generateSortedChunks(input: IBigFile): List[Future[IBigFile]] = {
      val mem = rs.remainingMemory
      println("Mem : " + mem)
      val blockSize = getBlockSize(mem)
      val inputSeq = divideChunk(input, blockSize, "sortedChunk")
      inputSeq.map(t => Future{sortChunk(t)}).toList
    }
  }


  class SlaveSorter {
    def run(inputDirs: List[String]): List[Future[IBigFile]] = {
      // initializing structs
      val input: IBigFile = new MultiFile(inputDirs)
      //val input : IBigFile = new ConstFile
      val rs:ResourceChecker = new ResourceChecker()
      val sorter: ChunkSorter = new SingleThreadSorter(rs)
      // operate on
      sorter.generateSortedChunks(input)
    }
  }

}
