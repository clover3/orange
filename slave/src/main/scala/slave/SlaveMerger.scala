package slave

import slave.future._
import slave.merger._
import slave.{ConcatFile, BigOutputFile, IBigFile}
import slave.Record._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math.Ordering

package object merger {

  val sortedFileName = "sorted"
  trait MergePQ {
    def getMin(): Record

    def isEmpty: Boolean
  }

  // Phase 2 of the sorting process
  trait ChunkMerger {
    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile
  }

  // TODO OS might not keep pages of the files, It might be better to read sequence of data
  class SingleThreadMerger extends ChunkMerger {
    var index = 0
    def getOutName(): String = {
      this.synchronized {
        index = index + 1
        val name = sortedFileName + index
        name
      }
    }

    def MergeWithPQ(sortedChunks: List[IBigFile]): IBigFile = {
      val output: BigOutputFile = new BigOutputFile(getOutName())

      class DataBag(sources: Vector[IBigFile]) {
        val cursorArray: ArrayBuffer[Int] = ArrayBuffer.empty ++ (for (i <- Range(0, sources.size)) yield 0)

        def getItem(index: Int): Option[Record] = {
          val cursor = cursorArray(index)
          if (cursor < sources(index).numOfRecords) {
            val rec = sources(index).getRecord(cursor)
            cursorArray(index) = cursorArray(index) + 1
            Some(rec)
          }
          else
            None
        }

        def isEmpty: Boolean = {
          Range(0, sources.size).forall(i => {
            sources(i).numOfRecords <= cursorArray(i)
          })
        }
      }

      class SimplePQ(sortedChunks: List[IBigFile]) extends MergePQ {

        object EntryOrdering extends Ordering[(Record, Int)] {
          def compare(a: (Record, Int), b: (Record, Int)) = b._1._1 compare a._1._1
        }

        val priorityQueue: mutable.PriorityQueue[(Record, Int)] = new mutable.PriorityQueue[(Record, Int)]()(EntryOrdering)
        val dataBag = new DataBag(sortedChunks.toVector)
        val contructor = {
          for (i <- Range(0, sortedChunks.size)) {
            val rec: Option[Record] = dataBag.getItem(i)
            rec match {
              case Some(r) => priorityQueue.enqueue((r, i))
              case None => ()
            }
          }
        }

        def getMin(): Record = {
          val (rec, i) = priorityQueue.dequeue()
          dataBag.getItem(i) match {
            case Some(r) => (priorityQueue += ((r, i)))
            case None => ()
          }
          rec
        }

        def isEmpty: Boolean = priorityQueue.isEmpty
      }

      val pq: MergePQ = new SimplePQ(sortedChunks)
      while (!pq.isEmpty) {
        output.appendRecord(pq.getMin())
      }
      output.close()
      output.toInputFile
    }

    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile = {
       MergeWithPQ(sortedChunks)
      //MergeSimple(sortedChunks)
    }

    def MergeSimple(sortedChunks: List[IBigFile]): IBigFile = {
      def getHead(file: IBigFile): Record = file.getRecord(0)
      val output: BigOutputFile = new BigOutputFile(getOutName())

      val nChunks = sortedChunks.size
      val minList: List[Record] = sortedChunks.map(getHead)
      val idxSeq: Seq[Int] = for (i <- Range(0, nChunks)) yield 0

      val arrChunks: ArrayBuffer[IBigFile] = ArrayBuffer.empty //
      val minArr: ArrayBuffer[Record] = ArrayBuffer.empty
      val idxArr: ArrayBuffer[Int] = ArrayBuffer.empty

      arrChunks ++= sortedChunks
      minArr ++= minList
      idxArr ++= idxSeq

      def getMinRec() = minArr.minBy(rec => rec.key)
      def updateMinArray(minRec: Record) = {
        val idxMinChunk: Int = minArr.indexOf(minRec) // idxMin represents the selected chunk
        idxArr(idxMinChunk) = idxArr(idxMinChunk) + 1
        if (arrChunks(idxMinChunk).numOfRecords <= idxArr(idxMinChunk)) {
          minArr.remove(idxMinChunk)
          idxArr.remove(idxMinChunk)
          arrChunks.remove(idxMinChunk)
        }
        else
          minArr(idxMinChunk) = arrChunks(idxMinChunk).getRecord(idxArr(idxMinChunk))
      }
      @tailrec
      def mergeIteration: Unit = {
        if (minArr.isEmpty)
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

  class DualThreadMerger extends ChunkMerger {
    def divide(chunks: List[IBigFile], num: Int): List[List[IBigFile]] = {
      def rearrange[A](listList : List[List[A]]) : List[List[A]] = {
        if( listList.head == Nil)
          Nil
        else {
          val heads : List[A]  = listList.map(l => l.head)
          val tails : List[List[A]] = listList.map(l => l.tail)
          heads :: rearrange(tails)
        }
      }

      def sampling(sample: IBigFile, num: Int) : List[String] = {
        val keyLimitMin = 0.toChar.toString * 10
        val keyLimitMax = 126.toChar.toString * 10
        val blockSize = sample.numOfRecords / num
        val midKeys: List[String] = (for (i <- 1 until num) yield {
          sample.getRecord(i * blockSize)._1
        }).toList
        keyLimitMin +: keyLimitMax +: midKeys
      }

      val keys: List[String] = sampling(chunks.head, num)

      def split(file :IBigFile) : List[IBigFile] = {
        val intervals = Splitter.makePartitionsListFromKey(file, keys)
        intervals.map(t => new PartialFile(file,t._1, t._2) )
      }
      rearrange(chunks.map(split))
    }

    def MergeBigChunk(sortedChunks: List[IBigFile]): IBigFile = {
      val merger = new SingleThreadMerger;
      val lst: List[List[IBigFile]] = divide(sortedChunks, 2)
      val futureList = lst.map(x => Future {
        merger.MergeBigChunk(x)
      })
      new ConcatFile(Await.result(all(futureList), Duration.Inf))
    }
  }
}