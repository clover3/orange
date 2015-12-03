package slave

import java.io.File

import common.future._

import slave.SlaveConfig.Config
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


  trait MergePQ {
    def getMin(): BRecord

    def isEmpty: Boolean
  }

  // Phase 2 of the sorting process
  trait ChunkMerger {
    def MergeBigChunk(sortedChunks: List[IBigFile]): Unit
  }

  // TODO OS might not keep pages of the files, It might be better to read sequence of data
  class SingleThreadMerger (val outputdir : String) extends ChunkMerger {
    var d = new File(outputdir)
    if (!d.exists)
      d.mkdir()
    val sortedFileName = outputdir + "/" + "sorted"
    var index = 0
    def getOutName(): String = {
      this.synchronized {
        index = index + 1
        val name = sortedFileName + index
        name
      }
    }

    def MergeWithPQ(sortedChunks: List[IBigFile]): Unit = {
      val name = getOutName()
      val output: BigOutputFile = new BigOutputFile(name)

      class DataBag(sources: Vector[IBigFile]) {
        val cursorArray: ArrayBuffer[Int] = ArrayBuffer.empty ++ (for (i <- Range(0, sources.size)) yield 0)

        def getItem(index: Int): Option[BRecord] = {
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
        object EntryOrdering extends Ordering[(BRecord, Int)] {
          def compare(a: (BRecord, Int), b: (BRecord, Int)) = BAOrdering.compare(b._1._1, a._1._1)
        }

        val priorityQueue: mutable.PriorityQueue[(BRecord, Int)] = new mutable.PriorityQueue[(BRecord, Int)]()(EntryOrdering)
        val dataBag = new DataBag(sortedChunks.toVector)
        val contructor = {
          for (i <- Range(0, sortedChunks.size)) {
            val rec: Option[BRecord] = dataBag.getItem(i)
            rec match {
              case Some(r) => priorityQueue.enqueue((r, i))
              case None => ()
            }
          }
        }

        def getMin(): BRecord = {
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
      var n = 0
      while (!pq.isEmpty) {
        output.appendRecord(pq.getMin())
        val unit = 1000000
        if( n % unit == 0)
          println(name + " : " + "=" * (n / unit) )
        n = n + 1
      }
      output.close()
      output
    }

    def MergeBigChunk(sortedChunks: List[IBigFile]): Unit = {
       MergeWithPQ(sortedChunks)
    }

    def MergeSimple(sortedChunks: List[IBigFile]): IBigFile = {
      def getHead(file: IBigFile): BRecord = file.getRecord(0)
      val output: BigOutputFile = new BigOutputFile(getOutName())

      val nChunks = sortedChunks.size
      val minList: List[BRecord] = sortedChunks.map(getHead)
      val idxSeq: Seq[Int] = for (i <- Range(0, nChunks)) yield 0

      val arrChunks: ArrayBuffer[IBigFile] = ArrayBuffer.empty //
      val minArr: ArrayBuffer[BRecord] = ArrayBuffer.empty
      val idxArr: ArrayBuffer[Int] = ArrayBuffer.empty

      arrChunks ++= sortedChunks
      minArr ++= minList
      idxArr ++= idxSeq

      def getMinRec() = minArr.minBy(rec => rec.key)
      def updateMinArray(minRec: BRecord) = {
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

  class DualThreadMerger(val outputdir : String) extends ChunkMerger {
    var d = new File(outputdir)
    if (!d.exists)
      d.mkdir()
    def divide(chunks: List[IBigFile])(implicit config:Config): List[List[IBigFile]] = {
      val num = config.numMergeThread
      def rearrange[A](listList : List[List[A]]) : List[List[A]] = {
        if( listList.head == Nil)
          Nil
        else {
          val heads : List[A]  = listList.map(l => l.head)
          val tails : List[List[A]] = listList.map(l => l.tail)
          heads :: rearrange(tails)
        }
      }

      def sampling(sample: IBigFile, num: Int) : List[ByteArray] = {
        val keyLimitMin = StringToByteArray(0.toChar.toString * 10)
        val keyLimitMax = StringToByteArray(126.toChar.toString * 10)
        val blockSize = sample.numOfRecords / num
        val midKeys: List[ByteArray] = (for (i <- 1 until num) yield {
          sample.getRecord(i * blockSize)._1
        }).toList
        val lst = keyLimitMin +: midKeys :+ keyLimitMax
        lst
      }

      val keys: List[ByteArray] = sampling(chunks.head, num)
      def split(file :IBigFile) : List[IBigFile] = {
        val intervals = Splitter.makePartitionsListFromKey(file, keys)
        intervals.map(t => new PartialFile(file,t._1, t._2) )
      }
      rearrange(chunks.map(split))
    }

    def MergeBigChunk(sortedChunks: List[IBigFile]): Unit = {
      val merger = new SingleThreadMerger(outputdir);
      val lst: List[List[IBigFile]] = divide(sortedChunks)
      val futureList = lst.map(x => Future {
        merger.MergeBigChunk(x)
      })
      (Await.result(all(futureList), Duration.Inf))
    }
  }
}

