/**
 * Created by Clover on 2015-10-30.
 */

package slave
import java.nio.ByteBuffer
import scala.math.Ordering
import scala.util.control.Breaks
import scala.util.control.Breaks._

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

  def printRecVector(vectRec : Vector[Record], num :Int) = {
    vectRec.slice(0, num).map( rec => println(rec.key) )
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

  type ByteArray = Array[Byte]
  def StringToByteArray(str:String):ByteArray = {
    str.toCharArray().map(x => x.toByte)
  }

  implicit object BAOrdering extends Ordering[ByteArray] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
      if (a eq null) {
        if (b eq null) 0
        else -1
      }
      else if (b eq null) 1
      else {
        val L = math.min(a.length, b.length)
        var i = 0
        while (i < L) {
          if (a(i) < b(i)) return -1
          else if (b(i) < a(i)) return 1
          i += 1
        }
        if (L < b.length) -1
        else if (L < a.length) 1
        else 0
      }
    }
  }
  //Vector[Record] => ByteBuffer (for sending in network)
  implicit class BRecordVectorCompanionOps(val vectorRecord: Vector[BRecord]) extends AnyVal {
    def toMyBuffer : ByteBuffer = {

      //record(key,data)=>String(key+data)=> Array[Array[Byte]]
      val recordVector : Array[Array[Byte]] = vectorRecord.map(rec => Array.concat(rec._1, rec._2)).toArray

      val byteArr: Array[Byte] = recordVector.flatten
      val size : Int = vectorRecord.size
      val buf = ByteBuffer.allocate(4)
      buf.putInt(size)
      val byteResult : Array[Byte] = buf.array() ++ byteArr
      val result = ByteBuffer.wrap(byteResult)
      result
    }
  }


  type BRecord = (ByteArray, ByteArray)
  implicit class BRecordCompanionOps(val rec:BRecord) extends AnyVal {
    def key = rec._1
    def data = rec._2
    def toStr:String = {
      "(" + rec._1.toStr + "," + rec._2.toStr + ")"
    }

    def min(r2:BRecord) : BRecord = {
      if (rec.key < r2.key)
        rec
      else
        r2
    }
  }

  implicit class ByteArrayCompanionOps(val ba: ByteArray) extends AnyVal{
    override
    def toString() :String = {
      new String(ba)
    }
    def toStr() :String = {
      new String(ba)
    }
    def hex() : String = {
      ba.map(b => "%d".format(b) ).mkString
    }
    def myEqual(that:ByteArray) = {
      val size = ba.size
      var i = 0
      var equal = true
      try{
        val loop = new Breaks;
        loop.breakable {
          while (i < size) {
            if (ba(i) != that(i)) {
              equal = false
              loop.break
            }
            i = i + 1
          }
        }
      }catch{
        case e : ArrayIndexOutOfBoundsException => {
          equal = false
        }
      }
      equal
    }
    def <(that: ByteArray) = {
      val size = ba.size
      var i = 0
      var less = false
      try {
        val loop = new Breaks;
        loop.breakable {
          while (i < size) {
            if (ba(i) < that(i)) {
              less = true
              loop.break
            }
            else if (ba(i) > that(i)) {
              less = false
              loop.break
            }
            i = i + 1
          }
        }
      }catch{
        case e : ArrayIndexOutOfBoundsException => less = ba.size < that.size
      }
      less
    }
    def >(that:ByteArray) = {
      that < ba
    }
    def >=(that:ByteArray) = {
      val res = (ba > that ) || (ba.myEqual(that) )
      res
    }
    def <=(that:ByteArray) = {
      val res = (ba < that ) || (ba.myEqual(that))
      res
    }
  }

}

