package slave

import slave.Record._
import scala.concurrent.duration.Duration
import scala.math._

import scala.concurrent.{Await, Promise, ExecutionContext, Future}
import scala.util.{Success, Failure}

/**
 * Created by Clover on 2015-11-01.
 */

package object SlaveSorter {

  // Phase 1 of the sorting process
  trait ChunkSorter {
    def generateSortedChunks(input: IBigFile): List[IBigFile]
  }

  // Phase 2 of the sorting process
  trait ChunkMerger {
    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile
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
        val data = sort(inFile, st, ed)
        val outfile : BigOutputFile = ???
        outfile.setRecords(data)
        outfile
      }
    }

    def sort(file: IBigFile, st: Int, ed: Int): Vector[Record] = ???

    def generateSortedChunks(input: IBigFile): List[IBigFile] = {
      val mem = rs.remainingMemory - 10 * rs.mb
      val blockSize = ???
      val inputSeq = divideChunk(input, blockSize, "sortedChunk")
      inputSeq.map(read_sort_write).toList
    }
  }

  class MultiThreadSorter extends ChunkSorter{
    def generateSortedChunks(input: IBigFile): List[IBigFile] = ???
  }


  class SlaveSorter {
    def run(inputDirs: List[String]): IBigFile = {
      val input: IBigFile = new BigInputFile(inputDirs)
      val merger: ChunkMerger = ???
      val sorter: ChunkSorter = ???
      val sortedChunks: List[IBigFile] = sorter.generateSortedChunks(input)
      merger.MergeBigChunk(sortedChunks)
    }
  }

}