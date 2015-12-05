package slave

import java.io.{FileNotFoundException, File}

import org.scalatest.FunSuite
import common.future._
import slave.merger.{DualThreadMerger, ChunkMerger, SingleThreadMerger}
import slave.sorter._

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
  val pathHDD_F = List("F:\\Test\\inputdir1","F:\\Test\\inputdir2")
  val pathMultiHDD = List("E:\\Test\\inputdir1","F:\\Test\\inputdir2")
  val pathServerBig = List("/scratch1/Orange/inputdir1")
  val tempDir = "temp"

  test("Sort Only"){
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathLocal)
    val rs:ResourceChecker = new ResourceChecker()
    val sorter = new MultiThreadSorter(rs, tempDir)

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(List(input))), Duration.Inf)
    }

    println("sort  time(ms) :"+ timeSort)
  }

  test("Sort MultiHDD"){
    //val input: IBigFile = new ConstFile
    val input: List[IBigFile] = pathMultiHDD.map(d => new MultiFile(List(d)))
    val rs:ResourceChecker = new ResourceChecker()
    val sorter = new MultiThreadSorter(rs, tempDir)

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(input)), Duration.Inf)
    }

    println("sort  time(ms) :"+ timeSort)
  }


  test("sorting test - Single+Single") {
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathLocal)
    val rs:ResourceChecker = new ResourceChecker()
    val merger: ChunkMerger = new SingleThreadMerger(".")
    val sorter = new SingleThreadSorter(rs, tempDir)

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(List(input))), Duration.Inf)
    }
    val (_, timeMerge) = profile{ merger.MergeBigChunk(sortedChunks) }

    println("sort  time(ms) :"+ timeSort)
    println("merge time(ms) :"+ timeMerge)
  }

  test("sorting test - Multi+Single") {
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathLocal)
    val rs:ResourceChecker = new ResourceChecker()

    val sorter = new MultiThreadSorter(rs, tempDir)
    val merger: ChunkMerger = new SingleThreadMerger(".")

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(List(input))), Duration.Inf)
    }
    val (_, timeMerge) = profile{ merger.MergeBigChunk(sortedChunks) }

    println("sort  time(ms) :"+ timeSort)
    println("merge time(ms) :"+ timeMerge)
  }

  test("Test:Single+Multi") {
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathServerBig)
    val rs:ResourceChecker = new ResourceChecker()
    val merger: ChunkMerger = new DualThreadMerger(".")
    val sorter = new SingleThreadSorter(rs, tempDir)

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(List(input))), Duration.Inf)
    }
    println("Generated " + sortedChunks.size + " sorted chunks")
    val (_, timeMerge) = profile{ merger.MergeBigChunk(sortedChunks) }

    println("sort  time(ms) :"+ timeSort)
    println("merge time(ms) :"+ timeMerge)
  }

  test("Test:Multi+Multi"){
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathServerBig)
    val rs:ResourceChecker = new ResourceChecker()
    val merger: ChunkMerger = new DualThreadMerger(".")
    val sorter = new MultiThreadSorter(rs, tempDir)

    // operate on
    val (sortedChunks, timeSort) = profile{
      Await.result(all(sorter.generateSortedChunks(List(input))), Duration.Inf)
    }
    println("Generated " + sortedChunks.size + " sorted chunks")
    val (_, timeMerge) = profile{ merger.MergeBigChunk(sortedChunks) }

    println("sort  time(ms) :"+ timeSort)
    println("merge time(ms) :"+ timeMerge)
  }

  test("Merge only"){
    def testsorted = {
      val d = new File(tempDir)
      d.listFiles.filter(_.isFile).toList.map {
        case f => new SingleFile(tempDir + "/" + f.getName)
      }
    }
    val merger: ChunkMerger = new DualThreadMerger(".")
    val (_, timeMerge) = profile{ merger.MergeBigChunk(testsorted) }
    println("merge time(ms) :"+ timeMerge)
  }

}

