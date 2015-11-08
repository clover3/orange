package slave

import org.scalatest.FunSuite
import slave.sorter._

/**
 * Created by Clover on 2015-11-06.
 */
class SorterSuite extends FunSuite {

  test("sorting test - virtual file") {
    val input: IBigFile = new ConstFile
    val rs:ResourceChecker = new ResourceChecker()
    val merger: ChunkMerger = new SingleThreadMerger
    val sorter = new SingleThreadSorter(rs)

    // operate on
    val sortedChunks: List[IBigFile] = sorter.generateSortedChunks(input)
//    merger.MergeBigChunk(sortedChunks)
  }

}
