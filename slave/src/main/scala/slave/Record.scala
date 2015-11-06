/**
 * Created by Clover on 2015-10-30.
 */

package slave
import java.nio.ByteBuffer

package object Record {

  val keyLimitMin = 0.toChar.toString * 10
  val keyLimitMax = 126.toChar.toString * 10

  type Record = (String, String)

  def parseRecordBuffer(buf : ByteBuffer) : Vector[Record] = {
    val arr = buf.array()
    val totallen = buf.limit()
    val recordnum = totallen / 100
    val result : Vector[Record] = (for (i <- Range(0, recordnum))
      yield {
        val idx0 = i * 100
        val idx1 = idx0 + 10
        val idx2 = idx1 + 90
        (new String(arr.slice(idx0, idx1)), new String(arr.slice(idx1, idx2)))
      }).toVector
    result
  }

  //Vector[Record] => ByteBuffer (for sending in network)
  implicit class RecordCompanionOps(val vectorRecord: Vector[Record]) extends AnyVal {
    def toBuffer : ByteBuffer = {

      //record(key,data)=>String(key+data)=> Array[Array[Byte]]
      val recordVector : Array[Array[Byte]] = vectorRecord.map(str => (str._1+str._2).getBytes("UTF-8")).toArray

      val byteArr: Array[Byte] = recordVector.flatten

      ByteBuffer.wrap(byteArr)
    }
  }

  implicit class PartitionCompanionOps(val rec: Record) extends AnyVal {
    def key: String = rec._1
    def data: String = rec._2

    def min(r2:Record) : Record = {
      if( rec.key < r2.key )
        rec
      else
        r2
    }
  }

  def printRecVector(vectRec : Vector[Record], num :Int) = {
    vectRec.slice(0, num).map( rec => println(rec.key) )
  }


}