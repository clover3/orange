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
  def getRecord(i: Int): Record

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record]



}

trait IBigFileWithCache extends IBigFile{

  //get i'th record from recordCache(using Cache->hit or miss)
  def getRecordByCached(i:Int) :Record

  //put records to Cache Frome File ( if Cache miss)
  def getRecordFromFileWithCache(i: Int): Record

  //min(a,b)
  def min(a: Int, b: Int): Int ={
    if (a > b) b
    else a
  }

  //get i'th record directly from file
  def getRecordDirect(i: Int): Record
}


// I add argument inputDirs(:List[String])
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
  def getRecord(i: Int): Record = {
    val fileIndex :Int = i/numOfRecords
    val recordIndex : Int = i%numOfRecords
    //define randomAccessFile just for read("r)
    val raf = new RandomAccessFile(fileList(fileIndex), "r")

    getRecordByCached(i)

  }

  def getRecordByCached(i:Int) :Record = {
    cache.getRecord(i) match {
      case None => getRecordFromFileWithCache(i)
      case Some(i) => i
    }
  }

  def getRecordFromFileWithCache(i: Int): Record ={
    val keyOffset :Long = 10
    val totalOffset :Long = 100
    val lineSize : Int = 100
    //set position

    val fileIndex :Int = i/numOfRecords
    val recordIndex : Int = i%numOfRecords
    val raf = new RandomAccessFile(fileList(fileIndex), "r")

    val pos = totalOffset * recordIndex
    raf.seek(pos)
    val nRecord = min(400, recordPerFile - i)
    val buf :Array[Byte] = new Array[Byte](lineSize*nRecord )
    raf.readFully(buf)

    val seq = for (i<- Range(0,nRecord)) yield {
      val st = i * lineSize
      val ed = st + lineSize
      val readline = new String(buf.slice(st, ed))
      val keyString = readline.take(keyOffset.toInt)
      val dataString = readline.drop(keyOffset.toInt)
      (keyString, dataString)
    }
    cache.addRecords(seq.toVector,i)
    cache.touch(i)
    seq.head
  }

  def getRecordDirect(i: Int): Record = {
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
      val nRecord = edRecord - stRecord
      file.seek(pos)
      val buf :Array[Byte] = new Array[Byte](recordSize * nRecord)
      file.readFully(buf)

      val seq = for( i <- Range(0, nRecord) ) yield {
        val st = i * recordSize
        val ed = st + recordSize
        val readline = new String(buf.slice(st, ed))
        val keyString = readline.take(keySize)
        val dataString = readline.drop(keySize)
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


  val raf = new RandomAccessFile(name, "r")
  val cache = new RecordCache
  lazy val numOfRecords: Int = raf.length().toInt / 100
  // get i'th record
  def getRecord(i: Int): Record = {
    //getRecordDirect(i)
    getRecordByCached(i)
  }

  def getRecordByCached(i: Int): Record = {
    cache.getRecord(i) match {
      case None => getRecordFromFileWithCache(i)
      case Some(r) => r
    }
  }

  def getRecordFromFileWithCache(i: Int): Record = {

    //set Offset for key or value
    //ex) AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111
    //    10 - 32 - 52
    val keyOffset: Long = 10
    val totalOffset: Long = 100
    val lineSize: Int = 100
    val pos = lineSize * i
    raf.seek(pos.toLong)
    val nRecord = min(400, numOfRecords - i)
    val buf: Array[Byte] = new Array[Byte](lineSize * nRecord)
    raf.readFully(buf)

    val seq = for (i <- Range(0, nRecord)) yield {
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
    val keyOffset: Long = 10
    val totalOffset: Long = 100
    val lineSize: Int = 100
    //set position
    val pos: Long = (totalOffset) * i
    raf.seek(pos)
    val buf: Array[Byte] = new Array[Byte](lineSize)
    raf.readFully(buf)


    val readline = new String(buf.take(100))
    val keyString = readline.take(keyOffset.toInt)
    val dataString = readline.drop(keyOffset.toInt)
    (keyString, dataString)
  }

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] = {

    val keyOffset: Long = 10
    val totalOffset: Long = 100
    val lineSize: Int = 100
    var pos: Long = 0
    pos = st * totalOffset
    raf.seek(pos)
    val buf: Array[Byte] = new Array[Byte](lineSize)
    val recordVector = for (i <- Range(st, ed)) yield {
      raf.readFully(buf)

      val readline = new String(buf.take(100))
      val keyString = readline.take(keyOffset.toInt)
      val dataString = readline.drop(keyOffset.toInt)

      (keyString, dataString): Record
    }
    recordVector.toVector
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



//////////////impelemented to test or improve performance,( no need for program execution!)//////////
//edit singleFile for test -> test well
class RecordCache2(name : String) {

  val blockSize = 1000
  val maxEntry = 50
  type CacheEntry = (Int, Int, Vector[Record])
  type CacheEntryF = (Int, Int, Future[Vector[Record]])
  var cacheVector : Vector[CacheEntry] = Vector.empty
  var cacheVectorF : Vector[CacheEntryF] = Vector.empty

  def touch(loc:Int) = {}

  val raf = new RandomAccessFile(name, "r")
  lazy val numOfRecords: Int = raf.length().toInt / 100
  val keyOffset :Long = 10
  val totalOffset :Long= 100
  val lineSize : Int = 100


  def min(a:Int,b:Int) :Int = {
    if(a > b) b
    else a
  }

  //File read func
  def readFile(pos:Int ,loc:Int) : Vector[Record]={
    raf.seek(pos.toLong)
    val nRecord = min(blockSize, numOfRecords-loc)
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
    seq.toVector
  }

  def addRecords(loc :Int) = {
    val pos = lineSize * (loc)
    val records : Vector[Record] = readFile(pos,loc)
    if( cacheVector.size < maxEntry ) {
      val entry = (loc, records.size, records)
      cacheVector =  cacheVector :+ entry
    }

  }

  //case loc == end of cache
  def addFutureRecords(loc:Int) :Unit  = {
    val pos = lineSize * (loc +1)
    def records : Vector[Record] = readFile(pos,loc+1)
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


  def getRecord(loc :Int) : Option[Record] = {
    val rec: Option[Record] = cacheVector.find(contain(loc)) match {
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
        val vector: Vector[Record] = e._3
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
          val vector : Vector[Record]= Await.result(e._3,Duration.Inf)
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


  val raf = new RandomAccessFile(name, "r")
  val cache = new RecordCache2(name)
  lazy val numOfRecords: Int = raf.length().toInt / 100

  // get i'th record
  def getRecord(i: Int): Record = {
    //getRecordDirect(i)
    cache.getRecord(i) match {
      case Some(r) => r
    }
  }



  def min(a: Int, b: Int): Int = {
    if (a > b) b
    else a
  }

  def getRecordDirect(i: Int): Record = {
    val keyOffset: Long = 10
    val totalOffset: Long = 100
    val lineSize: Int = 100
    val pos: Long = (totalOffset) * i
    raf.seek(pos)
    val buf: Array[Byte] = new Array[Byte](lineSize)
    raf.readFully(buf)

    val readline = new String(buf.take(100))
    val keyString = readline.take(keyOffset.toInt)
    val dataString = readline.drop(keyOffset.toInt)
    (keyString, dataString)
  }

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] = {

    val keyOffset: Long = 10
    val totalOffset: Long = 100
    val lineSize: Int = 100
    var pos: Long = 0
    pos = st * totalOffset
    raf.seek(pos)
    val buf: Array[Byte] = new Array[Byte](lineSize)
    val recordVector = for (i <- Range(st, ed)) yield {
      raf.readFully(buf)

      val readline = new String(buf.take(100))
      val keyString = readline.take(keyOffset.toInt)
      val dataString = readline.drop(keyOffset.toInt)

      (keyString, dataString): Record
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
class PartialFile( file:IBigFile, st:Int, ed:Int) extends IBigFile {
  def numOfRecords = (ed - st)
  def getRecord(i : Int) :Record = {
    file.getRecord(i + st)
  }
  def getRecords(stArg:Int, edArg: Int) = {
    file.getRecords( stArg + st, edArg + st)
  }
}
//////////////impelemented to test or improve performance,( no need for program execution!)//////////

trait IOutputFile {

  //set Vector[Record]
  def setRecords(records : Vector[Record]) : Future[Unit]

  //append record to eof
  def appendRecord(record: Record ) : Unit

  //convert IOutputFile => IBigFile
  def toInputFile : IBigFile

  //flush
  def close()
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
  //to be deleted
  def toInputFilePreFetch : IBigFile = {
    new SingleFilePreFetch(outputPath)
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

/////for test
class NullOutputFile extends IOutputFile {
  def setRecords(records : Vector[Record]) : Future[Unit] = Future{
  }
  def write(record: Record ) : Future[Unit] = Future {

  }
  def appendRecord(record: Record ) : Unit = ???
  def toInputFile : IBigFile = new ConstFile
  def close = ()
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
  /////for test
}


object Splitter {
  def makePartitionsList(file:IBigFile, partitions : Partitions) : List[(Int,Int)]={
    val keyList: List[String] = partitions.head._2::partitions.map({p => p._3})

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
