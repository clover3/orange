package slave

import org.scalatest.FunSuite
import slave.sorter._

/**
 * Created by Clover on 2015-11-06.
 */
class SorterSuite extends FunSuite {
  def profile[R](code: => R, t: Long = System.currentTimeMillis()) = (code, System.currentTimeMillis() - t)
  val pathLocal = List("inputdir1", "inputdir2")
  val pathHDD = List("E:\\Test\\inputdir1","E:\\Test\\inputdir2")
  test("sorting test - virtual file") {
    //val input: IBigFile = new ConstFile
    val input: IBigFile = new MultiFile(pathLocal)
    val rs:ResourceChecker = new ResourceChecker()
    val merger: ChunkMerger = new SingleThreadMerger
    val sorter = new SingleThreadSorter(rs)

    // operate on
    val (sortedChunks, timeSort) = profile{ sorter.generateSortedChunks(input) }
    val (_, timeMerge) = profile{ merger.MergeBigChunk(sortedChunks) }

    println("sort  time(ms) :"+ timeSort)
    println("merge time(ms) :"+ timeMerge)
  }

}
