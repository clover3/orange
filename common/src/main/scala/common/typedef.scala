package common

import java.nio.ByteBuffer

package object typedef {

  ///////////// Common Definition - must be shared between master and slave /////////////////////
  class Partition(val ip: String, val Startkey: String, val Endkey: String)
    extends (String, String, String)(ip, Startkey, Endkey) {
    def apply ()= new Partition ("","","")
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
        (b :Int)<- Range(0, slaveNum)
      } yield {
          val idx0 = b * totalOffset
          val idx1 = idx0 + Ipoffset
          val idx2 = idx1 + StartKeyoffset
          val idx3 = idx2 + EndKeyoffset
          new Partition(new String(arr.slice(idx0, idx1), "UTF-8").trim,
                        new String(arr.slice(idx1, idx2), "UTF-8"),
                        new String(arr.slice(idx2, idx3), "UTF-8"))
        }
      val partitions : Partitions = PartitionList.toList
      partitions

    }

  }
//partitions to Buffer To write (Ip , key[10],key[10]) (Partitons -> buffer)
  implicit class PartitionCompanionOps(val partitions: Partitions) extends AnyVal {
    def toByteBuffer : ByteBuffer = {
      //partitions.foreach(x=>(x._1 + x._2 + x._3).toArray )
      var sum : Array[Byte] = Array()

      var length = partitions.length
      val bytearr1 = for{
        a <- Range(0,length)
      } yield{
          val ip = partitions(a)._1
          val startkey = partitions(a)._2
          val endkey = partitions(a)._3

          val buf : ByteBuffer = ByteBuffer.allocate(15)
          val ipArr : Array[Byte] = buf.put(ip.getBytes()).array()
          val startkeyArr : Array[Byte] = startkey.getBytes()
          val endkeyArr : Array[Byte] = endkey.getBytes()
          sum = sum ++: ipArr ++: startkeyArr ++: endkeyArr
        }


      //partitions.foreach(x=> sum += x._1 + x._2 + x._3  )
      //val byteArr: Array[Byte] = sum.getBytes
      ByteBuffer.wrap(sum)

    }
    def print = {
      partitions.foreach(x => println("ip:"+ x.ip + " st:"+ x.Startkey + " ed:" + x.Endkey))
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
        val keyList = keyArrList.map(bArr => new String(bArr,"UTF-8") ).toList
        (numTotalKey, keyList)
      }
  }

  implicit class SampleCompanionOps(val sample: Sample) extends AnyVal  {
    // write down List[Samples] to buffer
    def toBuffer : ByteBuffer = {
      val numTotalKeys = sample._1
      val numTotalKeyArr : Array[Byte] = ByteBuffer.allocate(4).putInt(numTotalKeys).array()

      val numSampleKeys = sample._2.length
      val numSampleKeyArr : Array[Byte] = ByteBuffer.allocate(4).putInt(numSampleKeys).array()

      val keyList = sample._2
      val byteArrArr : Array[Array[Byte]] = keyList.map(str => str.getBytes("UTF-8")).toArray

  // numKey + keys....

      val byteArr: Array[Byte] = numTotalKeyArr ++: numSampleKeyArr ++: byteArrArr.flatten

      ByteBuffer.wrap(byteArr)
    }

    def print: Unit = {
      println("Num of Keys : " + sample._1)
      sample._2.map(key => println(key))
    }
  }

  implicit class SampleListCompanionOps(val s: List[Sample]) extends AnyVal {
    def toSample: Sample = {
      def add(s1: Sample, s2: Sample): Sample = {
        ((s1._1 + s2._1), s1._2 ::: s2._2)
      }
      s.foldRight((0, List[String]()))(add)
    }
  }

}

