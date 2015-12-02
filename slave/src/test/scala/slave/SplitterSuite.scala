package slave

import common.typedef.{Partition, Partitions}
import common.future._
import org.scalatest.FunSuite
import slave.Sampler.SlaveSampler
import slave.sorter._
import slave.util._
import slave.socket.PartitionSocket
import slave.sorter.MultiThreadSorter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Created by Clover on 2015-11-11.
 */
class SplitterSuite extends FunSuite {
  test("Splitter Test") {
    val strList = List("0000000000", "0000000004", "0000000006",
      "0000000008", "0000000009", "0000000010",
      "0000000015", "0000000020", "0000009999")
    val list = strList.dropRight(1).zip(strList.tail).map(t => new Partition("1.1.1.1", t._1, t._2))

    val partitions: Partitions = list
    val file: IBigFile = new SortedConstFile
    val lst = Splitter.makePartitionsList(file, partitions)

    partitions.print

    println("Intervals:")
    lst.foreach(t => println(t._1, t._2))
  }

  val tempDir = "temp"
  test("work to partition") {
    val master = "127.0.0.1:5959"
    val socket = new PartitionSocket(master)
    val inputDirs = List("inputdir1", "inputdir2")
    val outputDir = "outdir"

    val sampler = SlaveSampler(socket, inputDirs, outputDir)
    val partitions = sampler.getPartition
    print("this is partition : ");
    println(partitions)
    partitions.print

    val input: IBigFile = new MultiFile(inputDirs)
    val rs: ResourceChecker = new ResourceChecker()
    val sorter = new MultiThreadSorter(rs,tempDir)
    // operate on
    val sortedChunks = Await.result(all(sorter.generateSortedChunks(List(input))), Duration.Inf)

    val intervalList = Splitter.makePartitionsList(sortedChunks.head, partitions)
    intervalList.map(t => println(t))

  }
}

