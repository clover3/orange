package slave

/**
 * Created by Clover on 2015-10-30.
 */

import java.io._
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, MappedByteBuffer}

import common.typedef.Partitions
import slave.Record._
import slave.util._

import scala.collection.mutable.MutableList
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration


trait IBigFile {
  val recordPerFile = 327680
  // returns total number of records in this file
  def numOfRecords: Int

  // get i'th record
  def getRecord(i: Int): BRecord

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[BRecord]
}

trait IBigFileWithCache extends IBigFile{

  //get i'th record from recordCache(using Cache->hit or miss)
  def getRecordByCached(i:Int) :BRecord

  //put records to Cache Frome File ( if Cache miss)
  def getRecordFromFileWithCache(i: Int): BRecord

  //min(a,b)
  def min(a: Int, b: Int): Int ={
    if (a > b) b
    else a
  }

}

class MultiFile(inputDirs : List[String])  extends IBigFileWithCache{

  ///////////////////////////////////
  ///file Info
  val fileList : List[File] = inputDirs.flatMap(getFileList)
  val totalFileNum :Int = fileList.length
  val cache = new RecordCache

  def getFileList(dirPath: String) : List[File] ={
    val d = new File(dirPath)
    if (d.exists && d.isDirectory)
      d.listFiles.filter(_.isFile).toList
    else
      throw new FileNotFoundException
  }

  // returns total number of records in this file
  def numOfRecords: Int = recordPerFile * totalFileNum

  // get i'th record
  def getRecord(i: Int): BRecord = {
    getRecordByCached(i)
  }

  def getRecordByCached(i:Int) :BRecord = {
    cache.getRecord(i) match {
      case None => getRecordFromFileWithCache(i)
      case Some(i) => i
    }
  }

  def getRecordFromFileWithCache(i: Int): BRecord ={
    val keyOffset :Long = 10
    val totalOffset :Long = 100
    val lineSize : Int = 100
    //set position

    val fileIndex :Int = i/numOfRecords
    val recordIndex : Int = i%numOfRecords
    val rafbuf = {
      val raf = new RandomAccessFile(fileList(fileIndex), "r")
      val buf = raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length())
      raf.close()
      buf
    }

    val pos = lineSize * recordIndex
    val nRecord = min(400, recordPerFile - i)
    val buf :Array[Byte] = new Array[Byte](lineSize*nRecord )
    rafbuf.position()
    rafbuf.get(buf)

    val seq = for (i <- Range(0, nRecord)) yield {
      val st = pos + i * lineSize
      val ed = st + lineSize
      val strBuilder = new StringBuilder()
      def append(b: Byte, sb :StringBuilder) : StringBuilder = sb.append(b)
      val key =  buf.slice(st, st+ keyOffset.toInt)
      val data =  buf.slice(st+ keyOffset.toInt, ed)
      (key, data)
    }
    cache.addRecords(seq.toVector,i)
    cache.touch(i)
    seq.head
  }

  def getRecords(st: Int, ed: Int): Vector[BRecord] = {

    val fileIndexStart    :Int = st / recordPerFile
    val fileIndexEnd      :Int = ed / recordPerFile
    val recordIndexBegin  :Int = st % recordPerFile
    val recordIndexEnd    :Int = ed % recordPerFile
    val keySize = 10
    val recordSize = 100
    def readFile (file : RandomAccessFile, stRecord: Int, edRecord: Int) : IndexedSeq[BRecord] = {
      val rafbuf = file.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length())
      file.close()
      val pos = stRecord * recordSize
      val nRecord = edRecord - stRecord
      val buf :Array[Byte] = new Array[Byte](recordSize * nRecord)
      rafbuf.position(pos)
      rafbuf.get(buf)

      val seq = for (i <- Range(0, nRecord)) yield {
        val st = i * recordSize
        val ed = st + recordSize
        val keyString =  buf.slice(st, st+ keySize)
        val dataString =  buf.slice(st+ keySize, ed)
        (keyString, dataString)
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

class SingleFile(name : String) extends IBigFileWithCache {


  val rafbuf = {
    val raf = new RandomAccessFile(name, "r")
    val buf = raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length())
    raf.close()
    buf
  }
  val cache = new RecordCache
  lazy val numOfRecords: Int = rafbuf.limit().toInt / 100
  // get i'th record
  def getRecord(i: Int): BRecord = {
    //getRecordDirect(i)
    getRecordByCached(i)
  }

  def getRecordByCached(i: Int): BRecord = {
    cache.getRecord(i) match {
      case None => getRecordFromFileWithCache(i)
      case Some(r) => r
    }
  }

  def getRecordFromFileWithCache(i: Int): BRecord = {

    //set Offset for key or value
    //ex) AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111
    //    10 - 32 - 52
    val keySize = 10
    val recordSize = 100
    val pos = recordSize * i
    val nRecord = min(400, numOfRecords - i)
    val buf: Array[Byte] = new Array[Byte](recordSize * nRecord)
    rafbuf.position(pos)
    rafbuf.get(buf)

    val seq = for (i <- Range(0, nRecord)) yield {
      val st = i * recordSize
      val ed = st + recordSize
      val keyString =  buf.slice(st, st+ keySize)
      val dataString =  buf.slice(st+ keySize, ed)
      (keyString, dataString)
    }
    cache.addRecords(seq.toVector, i)
    cache.touch(i)
    seq.head
  }

  def getRecordDirect(i: Int): BRecord = {

    //define randomAccessFile just for read("r)

    //set Offset for key or value
    //ex) AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111
    //    10 - 32 - 52
    val keyOffset: Long = 10
    val totalOffset = 100
    val lineSize: Int = 100
    //set position
    val pos = (totalOffset) * i
    val buf: Array[Byte] = new Array[Byte](lineSize)
    rafbuf.position(pos)
    rafbuf.get(buf)

    (buf.take(10), buf.drop(10))
  }

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[BRecord] = {

    val keyOffset: Long = 10
    val totalOffset = 100
    val lineSize: Int = 100
    val pos = st * totalOffset
    val buf: Array[Byte] = new Array[Byte](lineSize)
    rafbuf.position(pos)
    val recordVector = for (i <- Range(st, ed)) yield {
      rafbuf.get(buf)
      (buf.take(10), buf.drop(10))
    }
    recordVector.toVector
  }
}

class ConcatFile( files : List[IBigFile]) extends IBigFile {
  def numOfRecords: Int = ???
  def getRecord(i: Int): BRecord = ???
  def getRecords(st: Int, ed: Int): Vector[BRecord] = ???
}

class RecordCache {
  val maxEntry = 50
  type CacheEntry = (Int, Int, Vector[BRecord])
  var cacheVector : Vector[CacheEntry] = Vector.empty

  def touch(loc:Int) = {}
  def addRecords(records :Vector[BRecord], loc :Int) = {
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
  def getRecord(loc :Int) : Option[BRecord] = {
    val rec :Option[BRecord] = cacheVector.find( contain(loc) ) match {
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

//////////////impelemented to test or improve performance,( no need for program execution!)//////////
class RecordCache2(name : String) {

  val blockSize = 1000
  val maxEntry = 50
  type CacheEntry = (Int, Int, Vector[BRecord])
  type CacheEntryF = (Int, Int, Future[Vector[BRecord]])
  var cacheVector : Vector[CacheEntry] = Vector.empty
  var cacheVectorF : Vector[CacheEntryF] = Vector.empty

  def touch(loc:Int) = {}

  val rafbuf = {
    val raf = new RandomAccessFile(name, "r")
    val buf = raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length())
    raf.close()
    buf
  }
  lazy val numOfRecords: Int = rafbuf.limit().toInt / 100
  val keyOffset :Long = 10
  val totalOffset :Long= 100
  val lineSize : Int = 100


  def min(a:Int,b:Int) :Int = {
    if(a > b) b
    else a
  }

  //File read func
  def readFile(pos:Int ,loc:Int) : Vector[BRecord]={
    val nRecord = min(blockSize, numOfRecords-loc)
    val buf :Array[Byte] = new Array[Byte](lineSize * nRecord)
    rafbuf.position(pos)
    rafbuf.get(buf)
    val seq = for( i <- Range(0, nRecord) ) yield {
      val st = i * lineSize
      val ed = st + lineSize
      val bufSub :Array[Byte] = buf.slice(st, ed)
      (bufSub.take(keyOffset.toInt), bufSub.drop(keyOffset.toInt))
    }
    seq.toVector
  }

  def addRecords(loc :Int) = {
    val pos = lineSize * (loc)
    val records : Vector[BRecord] = readFile(pos,loc)
    if( cacheVector.size < maxEntry ) {
      val entry = (loc, records.size, records)
      cacheVector =  cacheVector :+ entry
    }

  }

  //case loc == end of cache
  def addFutureRecords(loc:Int) :Unit  = {
    val pos = lineSize * (loc +1)
    def records : Vector[BRecord] = readFile(pos,loc+1)
    if( cacheVectorF.size < maxEntry ) { //maybe always cacheVectorF.size =1
    val entry = (loc, records.size, Future {records})
      cacheVectorF = cacheVectorF :+ entry
    }
  }

  def moveCacheFToCache(loc:Int)  : Unit= {

    //Await.result(cacheVectorF.find(containF(loc))._3 , Duration.Inf)
    val e = cacheVectorF.head
    val result : CacheEntry = (e._1,e._2, Await.result(e._3,Duration.Inf))

    //val result : Vector[CacheEntry]= cacheVectorF.map(x=>(x._1,x._2,Await.result(x._3,Duration.Inf)))
    cacheVector = cacheVector :+ result
    cacheVectorF = cacheVectorF.tail
  }



  def contain(loc :Int)(entry: CacheEntry) = {
    val begin = entry._1
    val end = entry._1 + entry._2
    begin <= loc && loc < end
  }
  // same func above func
  def containF(loc :Int)(entry: CacheEntryF) = {
    val begin = entry._1
    val end = entry._1 + entry._2
    begin <= loc && loc < end
  }

  def endOf(loc:Int)(entry: CacheEntry) = {
    val end = entry._1 + entry._2
    loc + 1 == end
  }

  //to make new cacheF
  def endOfcacheF(loc:Int)(vectorEntryF: Vector[CacheEntryF]) = {
    val end = vectorEntryF.last._1 + vectorEntryF.last._2
    loc + 1 == end
  }
  def hasRecord(loc :Int) : Boolean = {
    cacheVector.exists( contain(loc) )
  }

  def removeCheck(loc:Int): Unit = {
    cacheVector = cacheVector.filterNot(endOf(loc))
  }


  def getRecord(loc :Int) : Option[BRecord] = {
    val rec: Option[BRecord] = cacheVector.find(contain(loc)) match {
      case Some(e) => {
        //        if (endOf(loc)(e)){
        //          addFutureRecords(loc)
        //        }
        //=> endOf(loc)(e) is too slow => so I change code as Index
        if (loc%400 == 399){
          addFutureRecords(loc)
        }

        //case 1. record(i) exists in cache  =>find record i
        val index = loc - e._1
        val vector: Vector[BRecord] = e._3
        Option(vector(index))
      }
      case None => cacheVectorF.find(containF(loc)) match {
        //case 2. record(i) didn't exists in cache =>
        case None => {
          //case 2.1 record(i) didn't exists in cacheF =>make new cache
          addRecords(loc)
          val vector = cacheVector.last._3.head
          Option(vector)
        }
        case Some(e) => {
          //case 2.2 record(i) exists in cacheF
          val index = loc - e._1
          val vector : Vector[BRecord]= Await.result(e._3,Duration.Inf)
          moveCacheFToCache(loc) //delete now cacheF(Future) and add to cache(not future)
          Option(vector(index))
        }
      }
    }
    removeCheck(loc)
    rec
  }




}
class SingleFilePreFetch(name : String) extends IBigFile {


  val rafbuf = {
    val raf = new RandomAccessFile(name, "r")
    val buf = raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length())
    raf.close()
    buf
  }
  val cache = new RecordCache2(name)
  lazy val numOfRecords: Int = rafbuf.limit().toInt / 100

  // get i'th record
  def getRecord(i: Int): BRecord = {
    //getRecordDirect(i)
    cache.getRecord(i) match {
      case Some(r) => r
      case None => throw new Exception
    }
  }



  def min(a: Int, b: Int): Int = {
    if (a > b) b
    else a
  }

  def getRecordDirect(i: Int): BRecord = {
    val keyOffset: Long = 10
    val totalOffset = 100
    val lineSize: Int = 100
    val pos = (totalOffset) * i
    val buf: Array[Byte] = new Array[Byte](lineSize)
    rafbuf.position(pos)
    rafbuf.get(buf)

    (buf.take(10), buf.drop(10))
  }

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[BRecord] = {

    val keyOffset: Long = 10
    val totalOffset = 100
    val lineSize: Int = 100
    val pos = st * totalOffset
    val buf: Array[Byte] = new Array[Byte](lineSize)
    rafbuf.position(pos)
    val recordVector = for (i <- Range(st, ed)) yield {
      rafbuf.get(buf)
      (buf.take(10), buf.drop(10))
    }
    recordVector.toVector
  }
}
class ConstFile extends IBigFile{
  // returns total number of records in this file
  def numOfRecords: Int = 327680 * 5

  // get i'th record
  def getRecord(i: Int): BRecord = {
    val keyVal = 1000* 10000 - i
    val key = StringToByteArray("%010d".format(keyVal))
    val data = StringToByteArray("4" * 90)
    (key, data)
  }
  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[BRecord] =
  {
    val lst = Range(st, ed).map( x => getRecord(x))
    lst.toVector
  }
}

class SortedConstFile extends IBigFile{
  // returns total number of records in this file
  def numOfRecords: Int = 200

  // get i'th record
  def getRecord(i: Int): BRecord = {
    val keyVal:Int = i * 3
    val key = StringToByteArray("%010d".format(keyVal))
    val data = StringToByteArray("4" * 90)
    (key, data)
  }
  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[BRecord] =
  {
    val lst = Range(st, ed).map( x => getRecord(x))
    lst.toVector
  }
  /////for test
}
class PartialFile( file:IBigFile, st:Int, ed:Int) extends IBigFile {
  def numOfRecords = (ed - st)
  def getRecord(i : Int) :BRecord = {
    file.getRecord(i + st)
  }
  def getRecords(stArg:Int, edArg: Int) = {
    file.getRecords( stArg + st, edArg + st)
  }
}
//////////////impelemented to test or improve performance,( no need for program execution!)//////////

trait IOutputFile {

  //set Vector[Record]
  def setRecords(records : Vector[BRecord]) : Future[Unit]

  //append record to eof
  def appendRecord(record: BRecord ) : Unit

  //convert IOutputFile => IBigFile
  def toInputFile : IBigFile

  //flush
  def close()
}

class BigOutputFile(outputPath: String) extends  IOutputFile {
  val cacheSize = 10000
  val cachedRecord: MutableList[BRecord] = MutableList.empty
  val memoryMappedFile = {
    deleteIfExist(outputPath)
    new RandomAccessFile(outputPath, "rw");
  }
  var lastPos:Long = 0
  var size = 0

  def setRecords(records: Vector[BRecord]): Future[Unit] = Future{
    val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, records.size * 100);
    writeToBuf(out, records)
    memoryMappedFile.close()
    size = records.size
  }


  def appendRecord(record: BRecord): Unit = {
    cachedRecord += record
    size += 1
    if( cachedRecord.size >= cacheSize )
      flush()
  }

  def close() : Unit = {
    flush()
    memoryMappedFile.close()
  }

  def toInputFile : IBigFile = {
    new SingleFile(outputPath)
  }
  //to be deleted
  def toInputFilePreFetch : IBigFile = {
    new SingleFilePreFetch(outputPath)
  }

  def flush() = {
    appendRecords(cachedRecord.toVector)
    cachedRecord.clear()
  }

  def writeToBuf(out : MappedByteBuffer, records:Vector[BRecord]) = {
    for(i <- Range(0,records.size) )
    {
      val pair = records(i)
      val buf:ByteArray = Array.concat(pair._1, pair._2)
      out.put(buf)
    }
  }

  private def appendRecords(records :Vector[BRecord]) : Unit = {
    val filesize = memoryMappedFile.length()
    val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, lastPos , records.size * 100);
    out.position(0)
    writeToBuf(out, records)
    lastPos = lastPos + records.size * 100
  }



}

/////for test
class NullOutputFile extends IOutputFile {
  def setRecords(records : Vector[BRecord]) : Future[Unit] = Future{
  }
  def write(record: BRecord ) : Future[Unit] = Future {

  }
  def appendRecord(record: BRecord ) : Unit = ???
  def toInputFile : IBigFile = new ConstFile
  def close = ()
}



object Splitter {
  def makePartitionsListFromKey(file:IBigFile, keyList: List[ByteArray]) : List[(Int,Int)]={
    // returns List[index of key in file that is same or bigger than given key]
    def getIndexList(keyList : List[ByteArray], st:Int, ed:Int) : List[Int] = {
      if( keyList.isEmpty )
        Nil
      else if( st + 1 >= ed)
      {
        val n = if( ed < file.numOfRecords ) st
        else ed
        keyList.map(_ => n)
      }
      else {
        val mid = (st + ed) / 2
        val midKey = file.getRecord(mid-1)._1
        val leftKey = keyList.filter(key => key <= midKey)
        val rightKey = keyList.filter(key => key > midKey)
        assert( keyList.size == (leftKey.size+rightKey.size) )
        val l1 = getIndexList(leftKey, st, mid )
        val l2 = getIndexList(rightKey, mid , ed)
        l1 ::: l2
      }
    }

    val indexList = getIndexList(keyList, 0, file.numOfRecords)
    val startList = indexList.dropRight(1)
    val endList = indexList.tail
    val result = startList.zip(endList)
    result
  }

  def makePartitionsList(file:IBigFile, partitions : Partitions) : List[(Int,Int)]={
    val strList =  partitions.head._2::partitions.map({p => p._3})
    val keyList: List[ByteArray] = strList.map(str => str.toCharArray().map(x => x.toByte))
    makePartitionsListFromKey(file, keyList)
  }
}
