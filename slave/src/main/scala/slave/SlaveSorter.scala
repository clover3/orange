package slave

import slave.Record._
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

package object SlaveSorter {

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
      val output : BigOutputFile = ???

      val nChunks = sortedChunks.size
      val minList :List[Record]= sortedChunks.map(getHead)
      val idxSeq :Seq[Int] = for( i <- Range(0, nChunks) ) yield 0

      val arrChunks: ArrayBuffer[IBigFile] = ArrayBuffer.empty
      val minArr: ArrayBuffer[Record] = ArrayBuffer.empty
      val idxArr: ArrayBuffer[Int] = ArrayBuffer.empty

      arrChunks++= sortedChunks
      minArr++= minList
      idxArr++= idxSeq

      @tailrec
      def mergeIteration : Unit = {
        val minRec = minArr.minBy( rec => rec.key )
        val idxMin = idxArr.indexOf(minRec)
        idxArr(idxMin) = idxArr(idxMin) + 1
        if( arrChunks(idxMin).numOfRecords <= idxSeq(idxMin) )
        {
          minArr.remove(idxMin)
          idxArr.remove(idxMin)
          arrChunks.remove(idxMin)
        }

        minArr(idxMin) = arrChunks(idxMin).getRecord( idxArr(idxMin) )
        //output.write(minRec)

        if( minArr.isEmpty )
          return
        else
          mergeIteration
      }
      mergeIteration
      output.toInputFile
    }
  }

  def serialiseFutures[A, B](l: Iterable[A])(fn: A => Future[B])
                            (implicit ec: ExecutionContext): Future[List[B]] = {
    l.foldLeft(Future(List.empty[B])) {
      (previousFuture, next) =>
        for {
          previousResults <- previousFuture
          next <- fn(next)
        } yield previousResults :+ next
    }
  }

  def all[T](fs: List[Future[T]])(implicit ec: ExecutionContext): Future[List[T]] = {
    val p = Promise[List[T]]()
    fs match {
      case head::tail => {
        head onComplete{
          case Failure(e) => p.failure(e)
          case Success(x) => all(tail) onComplete {
            case Failure(e) => p.failure(e)
            case Success(y) => p.success(x :: y)
          }
        }
      }
      case Nil => p.success(Nil)
    }
    p.future
  }

  def getBlockSize(memSize :Int) : Int = {
    1000 * 10000
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
        val data = Await.result(read_sort(inFile, st, ed), Duration.Inf)
//        printRecVector(data, 10)
        val outfile : IOutputFile = new BigOutputFile_temp(tuple._2)
        Await.result(outfile.setRecords(data), Duration.Inf)
        outfile.toInputFile
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
      val inputSeq = divideChunk(input, blockSize, "sortedChunk")
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
        val outfile: IOutputFile = ??? /// new BigOutputFile(outfileName)
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
      val input: IBigFile = new BigInputFile(inputDirs)
      val rs:ResourceChecker = new ResourceChecker()
      val merger: ChunkMerger = new SingleThreadMerger
      val sorter: ChunkSorter = new SingleThreadSorter(rs)

      // operate on
      val sortedChunks: List[IBigFile] = sorter.generateSortedChunks(input)
      merger.MergeBigChunk(sortedChunks)
    }
  }

}
