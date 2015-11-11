package slave

import common.typedef.{Partition, Partitions}
import org.scalatest.FunSuite

/**
 * Created by Clover on 2015-11-11.
 */
class SplitterSuite extends FunSuite {
  test("Partion Test"){
    val strList = List("0000000000","0000000004", "0000000006",
      "0000000008","0000000009","0000000010",
    "0000000015", "0000000020", "0000009999")
    val list = strList.dropRight(1).zip(strList.tail).map( t=> new Partition("1.1.1.1", t._1, t._2))

    val partitions: Partitions = list
    val file:IBigFile = new SortedConstFile
    val lst = Splitter.makePartitionsList(file, partitions)

    partitions.print

    println("Intervals:")
    lst.foreach( t=> println(t._1, t._2))
  }


}
