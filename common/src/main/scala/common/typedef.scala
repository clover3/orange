package common

import java.nio.{ByteBuffer, Buffer}

package object typedef {

  ///////////// Common Definition - must be shared between master and slave /////////////////////
  class Partition(val ip : String, val Startkey : String, val Endkey : String)
    extends (String, String, String)(ip,Startkey,Endkey){
  }
  type Partitions = List[Partition]
  // Sample = (Number of total records, sampled records)
  type Sample = (Int, List[String])
  type Samples = List[Sample]


  class BufferCorruptedException extends Exception

  // This function receives buffer parsed by  PartitionCompnionOps
  // buffer contains Parition information (buffer -> Partitions)
  def parsePartitionBuffer(buf : ByteBuffer ) : Partitions = {
    val arr:Array[Byte] = buf.array()
    val Ipoffset : Int = 15
    val StartKeyoffset : Int = 10
    val EndKeyoffset : Int = 10
    val totalOffset : Int = Ipoffset+ StartKeyoffset + EndKeyoffset
    val slaveNum :Int= arr.length/ totalOffset
    val expectLen = (Ipoffset+ StartKeyoffset + EndKeyoffset) * slaveNum
    if (arr.length != expectLen){
      throw new BufferCorruptedException
    } else{
      val PartitionList = for{
        (b :Int)<- (0, slaveNum)
      } yield {
          (arr.slice(b*totalOffset, b*totalOffset+Ipoffset ).toString ,arr.slice(b*totalOffset + Ipoffset ,b*totalOffset+Ipoffset+StartKeyoffset).toString , arr.slice(b*totalOffset+Ipoffset+StartKeyoffset, b*totalOffset+Ipoffset+StartKeyoffset +EndKeyoffset ).toString)
        }
      val partitions = PartitionList
      partitions

    }

  }
//partitions to Buffer To write (Ip , key[10],key[10]) (Partitons -> buffer)
  implicit class PartitionCompanionOps(val partitions: Partitions) extends AnyVal {
    def toBuffer : ByteBuffer = {
      //partitions.foreach(x=>(x._1 + x._2 + x._3).toArray )
      var sum : String =""

      partitions.foreach(x=> sum += x._1 + x._2 + x._3  )
      val byteArr: Array[Byte] = sum.getBytes
      ByteBuffer.wrap(byteArr)

    }
  }


  def parseSampleBuffer(buf : ByteBuffer) : Sample = {
      val arr:Array[Byte] = buf.array()
      val numTotalKey = ByteBuffer.wrap(arr.slice(0,4)).getInt()
      val numSampleKey = ByteBuffer.wrap(arr.slice(4,8)).getInt()
      val expectLen = 8 + numSampleKey * 10
      if( arr.length != expectLen ) {
        throw new BufferCorruptedException
      }
      else{
        val offset = 8
        val keyArrList = for {
          b <- Range(0, numSampleKey)
        } yield  arr.slice(offset + b*10, offset + b*10 + 10 )
        val keyList = keyArrList.map(bArr => bArr.mkString).toList
        (numSampleKey, keyList)
      }
  }

  implicit class SampleCompanionOps(val sample: Sample) extends AnyVal  {
    // write down List[Samples] to buffer
    def toBuffer : ByteBuffer = {
      val numTotalKeys = sample._1
      val numTotalKeyArr : Array[Byte] = ByteBuffer.allocate(4).putInt(numTotalKeys).array()

      val numKeys = sample._2.length
      val numKeyArr : Array[Byte] = ByteBuffer.allocate(4).putInt(numKeys).array()

      val keyList = sample._2
      val byteArrArr : Array[Array[Byte]] = keyList.map(str => str.getBytes).toArray


      val byteArr: Array[Byte] = numKeys.toByte +: byteArrArr.flatten
  // numKey + keys....

      val byteArr: Array[Byte] = numTotalKeyArr ++: numKeyArr ++: byteArrArr.flatten

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
