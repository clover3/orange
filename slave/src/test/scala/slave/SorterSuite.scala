package slave

import org.scalatest.FunSuite
import slave.sorter._
import slave.future._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Created by Clover on 2015-11-06.
 */
class SorterSuite extends FunSuite {
  def profile[R](code: => R, t: Long = System.currentTimeMillis()) = (code, System.currentTimeMillis() - t)
  val pathLocal = List("inputdir1", "inputdir2")
  val pathHDD = List("E:\\Test\\inputdir1","E:\\Test\\inputdir2")
  val pathMultiHDD = List("E:\\Test\\inputdir1","F:\\Test\\inputdir2")


  test("sorting test - Single+Single") {
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathLocal)
    val rs:ResourceChecker = new ResourceChecker()
    val merger: ChunkMerger = new SingleThreadMerger
    val sorter = new SingleThreadSorter(rs)

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(input)), Duration.Inf)
    }
    val (_, timeMerge) = profile{ merger.MergeBigChunk(sortedChunks) }

    println("sort  time(ms) :"+ timeSort)
    println("merge time(ms) :"+ timeMerge)
  }

  test("sorting test - Multi+Single") {
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathLocal)
    val rs:ResourceChecker = new ResourceChecker()

    val sorter = new MultiThreadSorter(rs)
    val merger: ChunkMerger = new SingleThreadMerger

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(input)), Duration.Inf)
    }
    val (_, timeMerge) = profile{ merger.MergeBigChunk(sortedChunks) }

    println("sort  time(ms) :"+ timeSort)
    println("merge time(ms) :"+ timeMerge)
  }


}

