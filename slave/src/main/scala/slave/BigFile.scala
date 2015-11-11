package slave

/**
 * Created by Clover on 2015-10-30.
 */

import java.io._
import java.nio.{MappedByteBuffer, ByteBuffer}
import java.nio.channels.{Channels, FileChannel}
import java.nio.charset.Charset

import common.typedef.Partitions
import slave.Record._
import slave.util._

import scala.collection.mutable.MutableList
import scala.collection.parallel.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future



trait IBigFile {
    val recordPerFile = 327680
    // returns total number of records in this file
    def numOfRecords: Int

    // get i'th record
    def getRecord(i: Int): Record

    // return collection of records
    // starting from st to ed  ( it should not include ed'th record )
    def getRecords(st: Int, ed: Int): Vector[Record]

}


// I add argument inputDirs(:List[String])
class MultiFile(inputDirs : List[String])  extends IBigFile{

  ///////////////////////////////////
  ///file Info
  def getFileList(dirPath: String) : List[File] ={
    val d = new File(dirPath)
    if (d.exists && d.isDirectory)
      d.listFiles.filter(_.isFile).toList
    else
      throw new FileNotFoundException
   }

  val fileList : List[File] = inputDirs.flatMap(getFileList)

  //for access to wanted index
  val totalFileNum :Int = fileList.length
  ////////////////////////////////////////////////

  // returns total number of records in this file
  def numOfRecords: Int = recordPerFile * totalFileNum

  // get i'th record
  def getRecord(i: Int): Record = {
    val fileIndex :Int = i/numOfRecords
    val recordIndex : Int = i%numOfRecords

    //define randomAccessFile just for read("r)
    val raf = new RandomAccessFile(fileList(fileIndex), "r")

    //set Offset for key or value
    //ex) AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111
    //    10 - 32 - 52
    val keyOffset :Long = 10
    val totalOffset :Long = 100
    val lineSize : Int = 100
    val buf :Array[Byte] = new Array[Byte](lineSize )

    //set position
    val pos :Long = (totalOffset) * recordIndex
    raf.seek(pos)
    raf.readFully(buf)
    val readline = new String(buf)
    val keyString = readline.take(keyOffset.toInt)
    val dataString = readline.drop(keyOffset.toInt)
    (keyString, dataString)
  }

  def getRecords(st: Int, ed: Int): Vector[Record] = {
    val fileIndexStart    :Int = st / recordPerFile
    val fileIndexEnd      :Int = ed / recordPerFile
    val recordIndexBegin  :Int = st % recordPerFile
    val recordIndexEnd    :Int = ed % recordPerFile
    val keySize = 10
    val recordSize = 100
    def readFile (file : RandomAccessFile, stRecord: Int, edRecord: Int) : IndexedSeq[Record] = {
      val pos = stRecord * recordSize
      file.seek(pos)
      val buf: Array[Byte] = new Array[Byte](recordSize)
      val seq = for (i <- Range(stRecord, edRecord)) yield {
        file.readFully(buf)
        val readline = new String(buf)
        val keyString = readline.take(keySize)
        val dataString = readline.drop(keySize)
        (keyString, dataString): Record
      }
      seq
    }

    def getRange(fileIndex :Int) : (Int, Int,Int) = {
      if( fileIndex < fileIndexStart || fileIndex > fileIndexEnd )
        throw new IndexOutOfBoundsException()
      val st = {
        if (fileIndex == fileIndexStart) recordIndexBegin
        else 0
      }
      val ed = {
        if (fileIndex == fileIndexEnd ) recordIndexEnd
        else recordPerFile
      }
      (fileIndex, st,ed)
    }

    val coverage: Seq[(Int,Int,Int)] = Range(fileIndexStart, fileIndexEnd+1).map( i => getRange(i)).filter( t => t._2 < t._3)
    coverage.flatMap( t => {
      readFile(new RandomAccessFile(fileList(t._1), "r"), t._2, t._3)
    }).toVector
  }

}

class RecordCache {
  val maxEntry = 50
  type CacheEntry = (Int, Int, Vector[Record])
  var cacheVector : Vector[CacheEntry] = Vector.empty
  def touch(loc:Int) = {}
  def addRecords(records :Vector[Record], loc :Int) = {
    if( cacheVector.size < maxEntry ) {
      val entry = (loc, records.size, records)
      cacheVector = entry +: cacheVector
    }
  }
  def contain(loc :Int)(entry: CacheEntry) = {
    val begin = entry._1
    val end = entry._1 + entry._2
    begin <= loc && loc < end
  }
  def endOf(loc:Int)(entry: CacheEntry) = {
    val end = entry._1 + entry._2
    loc + 1 == end
  }
  def hasRecord(loc :Int) : Boolean = {
    cacheVector.exists( contain(loc) )
  }

  def removeCheck(loc:Int): Unit = {
    cacheVector = cacheVector.filterNot(endOf(loc))
  }

  def getRecord(loc :Int) : Option[Record] = {
    val rec :Option[Record] = cacheVector.find( contain(loc) ) match {
      case None => None
      case Some(e) => {
        val vector = e._3
        val index = loc - e._1
        Option(vector(index))
      }
    }
    removeCheck(loc)
    rec
  }

}

class SingleFile(name : String) extends IBigFile {

  val raf = new RandomAccessFile(name, "r")
  val cache = new RecordCache
  lazy val numOfRecords: Int = raf.length().toInt / 100

  // get i'th record
  def getRecord(i: Int): Record = {
    //getRecordDirect(i)
    getRecordByCached(i)
  }
  def getRecordByCached(i:Int): Record = {
    cache.getRecord(i) match {
      case None => getRecordFromFileWithCache(i)
      case Some(r) => r
    }
  }
  def min(a:Int,b:Int) :Int = {
    if(a > b) b
    else a
  }

  def getRecordFromFileWithCache(i:Int) : Record = {
    //define randomAccessFile just for read("r)

    //set Offset for key or value
    //ex) AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111
    //    10 - 32 - 52
    val keyOffset :Long = 10
    val totalOffset :Long= 100
    val lineSize : Int = 100
    //set position
    val pos = lineSize * i
    raf.seek(pos.toLong)
    val nRecord = min(400, numOfRecords-i)
    val buf :Array[Byte] = new Array[Byte](lineSize * nRecord)
    raf.readFully(buf)

    val seq = for( i <- Range(0, nRecord) ) yield {
      val st = i * lineSize
      val ed = st + lineSize
      val readline = new String(buf.slice(st, ed))
      val keyString = readline.take(keyOffset.toInt)
      val dataString = readline.drop(keyOffset.toInt)
      (keyString, dataString)
    }
    cache.addRecords(seq.toVector, i)
    cache.touch(i)
    seq.head
  }

  def getRecordDirect(i: Int): Record = {

    //define randomAccessFile just for read("r)

    //set Offset for key or value
    //ex) AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111
    //    10 - 32 - 52
    val keyOffset :Long = 10
    val totalOffset :Long= 100
    val lineSize : Int = 100
    //set position
    val pos :Long = (totalOffset) * i
    raf.seek(pos)
    val buf :Array[Byte] = new Array[Byte](lineSize)
    raf.readFully(buf)

    val readline = new String(buf.take(100))
    val keyString = readline.take(keyOffset.toInt)
    val dataString = readline.drop(keyOffset.toInt)
    (keyString, dataString)
  }
  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] =
  {
    val keyOffset :Long = 10
    val totalOffset :Long = 100
    val lineSize:Int =100
    var pos : Long =0
    pos =st*totalOffset
    raf.seek(pos)
    val buf :Array[Byte] = new Array[Byte](lineSize)
    val recordVector = for(i <- Range(st,ed)) yield {
      raf.readFully(buf)
      val readline = new String(buf.take(100))
      val keyString = readline.take(keyOffset.toInt)
      val dataString = readline.drop(keyOffset.toInt)
      (keyString, dataString) : Record
      }
      recordVector.toVector
  }

}


class ConstFile extends IBigFile{
  // returns total number of records in this file
  def numOfRecords: Int = 327680 * 2

  // get i'th record
  def getRecord(i: Int): Record = {
    val keyVal = 1000* 10000 - i
    val keyString = "%010d".format(keyVal)
    val dataString = "7" * 90
    (keyString, dataString)
  }
  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] =
  {
    val lst = Range(st, ed).map( x => getRecord(x))
    lst.toVector
  }
}

trait IOutputFile {
  def setRecords(records : Vector[Record]) : Future[Unit]
  def appendRecord(record: Record ) : Unit
  def toInputFile : IBigFile
  def close()
}

class NullOutputFile extends IOutputFile {
  def setRecords(records : Vector[Record]) : Future[Unit] = Future{
  }
  def write(record: Record ) : Future[Unit] = Future {

  }
  def appendRecord(record: Record ) : Unit = ???
  def toInputFile : IBigFile = new ConstFile
  def close = ()
}

class AppendOutputFile(outputPath: String) {
  val dummyRec = ("", "")
  val cacheSize = 1000
  val cachedRecord: MutableList[Record] = MutableList.empty
  var vect : Vector[Record] = Vector.empty
  var index:Int = 0
  val ostream: FileOutputStream = {
    new FileOutputStream(new File(outputPath))
  }

  def setRecords(records: Vector[Record]): Future[Unit] = ???

  def flush() = {
    //for (i <- Range(0, cachedRecord.size)) {
    var i = 0
    while( i < vect.size ){
      val pair = vect(i)
      val text = (pair._1 + pair._2 + "\n")
      val buf = text.toCharArray().map(x => x.toByte)
      //val buf = ByteBuffer.wrap(text.getBytes)
      ostream.write(buf)
      i = i + 1
    }
    //cachedRecord.clear()
    vect = Vector.empty
  }

  def appendRecord(record: Record): Unit = {
    //cachedRecord += record
    vect = vect :+ record
    //if( cachedRecord.size >= 100 )
    if( vect.size >= 1000)
      flush()

  }
  def close() : Unit = {
    flush()
  }
  def toInputFile : IBigFile = {
    new SingleFile(outputPath)
  }
}

class BigOutputFile(outputPath: String) extends  IOutputFile {
  val cacheSize = 10000
  val cachedRecord: MutableList[Record] = MutableList.empty
  val memoryMappedFile = {
    deleteIfExist(outputPath)
    new RandomAccessFile(outputPath, "rw");
  }
  var lastPos = 0
  var size = 0

  def setRecords(records: Vector[Record]): Future[Unit] = Future{
    val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, records.size * 100);
    writeToBuf(out, records)
    memoryMappedFile.close()
    size = records.size
  }

  def appendRecord(record: Record): Unit = {
    cachedRecord += record
    size += 1
    if( cachedRecord.size >= cacheSize )
      flush()
  }

  def close() : Unit = {
    flush()
  }

  def toInputFile : IBigFile = {
    new SingleFile(outputPath)
  }

  def flush() = {
    appendRecords(cachedRecord.toVector)
    cachedRecord.clear()
  }

  def writeToBuf(out : MappedByteBuffer, records:Vector[Record]) = {
    for(i <- Range(0,records.size) )
    {
      val pair = records(i)
      val str = (pair._1 + pair._2 )
      val dbg = str.getBytes
      val buf = ByteBuffer.wrap(str.getBytes)
      out.put(buf)
    }
  }

  private def appendRecords(records :Vector[Record]) : Unit = {
    val filesize = memoryMappedFile.length()
    val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, lastPos + records.size * 100);
    out.position(lastPos)
    writeToBuf(out, records)
    lastPos = out.position()
  }

}

class SortedConstFile extends IBigFile{
  // returns total number of records in this file
  def numOfRecords: Int = 200

  // get i'th record
  def getRecord(i: Int): Record = {
    val keyVal:Int = i * 3
    val keyString = "%010d".format(keyVal)
    val dataString = "4" * 90
    (keyString, dataString)
  }
  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] =
  {
    val lst = Range(st, ed).map( x => getRecord(x))
    lst.toVector
  }
}


object Splitter {
  def makePartitionsList(file:IBigFile, partitions : Partitions) : List[(Int,Int)]={
    val keyList: List[String] = partitions.head._1::partitions.map({p => p._2})

    // returns List[index of key in file that is same or bigger than given key]
    def getIndexList(keyList : List[String], st:Int, ed:Int) : List[Int] = {
      if( keyList.isEmpty )
        Nil
      else if( st + 1 >= ed){
        val n = if( ed < file.numOfRecords ) st
        else ed
        keyList.map(_ => n)
      }
      else {
        val mid = (st + ed) / 2
        val midKey = file.getRecord(mid-1)._1
        val leftKey = keyList.filter(key => key <= midKey)
        val rightKey = keyList.filter(key => key > midKey)
        getIndexList(leftKey, st, mid ) ::: getIndexList(rightKey, mid , ed)
      }
    }

    val indexList = getIndexList(keyList, 0, file.numOfRecords)
    val startList = indexList.dropRight(1)
    val endList = indexList.tail
    startList.zip(endList)
  }
}