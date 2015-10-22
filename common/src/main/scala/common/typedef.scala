package common

import java.nio.{ByteBuffer, Buffer}

package object typedef {

  ///////////// Common Definition - must be shared between master and slave /////////////////////
  class Partition
  type Partitions = List[Partition]
  // Sample = (Number of total records, sampled records)
  type Sample = (Int, List[String])
  type Samples = List[Sample]

  implicit class SampleCompanionOps(val sample: Sample) extends AnyVal  {
    // write down List[Samples] to buffer
    def toBuffer : ByteBuffer = {
      val numKeys : Int = sample._1

      val keyList = sample._2
      val byteArrArr : Array[Array[Byte]] = keyList.map(str => str.getBytes).toArray

      val byteArr: Array[Byte] = numKeys.toByte +: byteArrArr.flatten

      ByteBuffer.wrap(byteArr)
    }
  }
  implicit class SampleListCompanionOps(val s: List[Sample]) extends AnyVal {
      def toSample: Sample = {
        def add(s1: Sample, s2: Sample): Sample = {
        ((s1._1 + s2._1), s1._2 ::: s2._2)
      }
      s.foldRight ((0, List[String] () ) ) (add)
    }
  }
}
