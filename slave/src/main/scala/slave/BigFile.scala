package slave

/**
 * Created by Clover on 2015-10-30.
 */

import java.io.{File, FileNotFoundException, RandomAccessFile}
import java.nio.{MappedByteBuffer, ByteBuffer}
import java.nio.channels.FileChannel

import slave.Record._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


trait IBigFile {
    // returns total number of records in this file
    def numOfRecords: Int

    // get i'th record
    def getRecord(i: Int): Record

    // return collection of records
    // starting from st to ed  ( it should not include ed'th record )
    def getRecords(st: Int, ed: Int): Vector[Record]

    // return index of key in sorted file
    def getIndexofKey(key : String) :  Int

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

  val fileList = inputDirs.flatMap(getFileList)

  //for access to wanted index
  val totalFileNum :Int = fileList.length
  ////////////////////////////////////////////////

  // returns total number of records in this file
  def numOfRecords: Int = 327680

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
    val valueOffset1 :Long = 32
    val valueOffset2 :Long = 52
    //num 2 blank for each blank !?!?
    //val totalOffset :Long = keyOffset + 2 + valueOffset1 + 2 + valueOffset2
    val totalOffset :Long = 100

    //set position
    val pos :Long = (totalOffset) * recordIndex
    raf.seek(pos)

    //val keyVal = raf.readLine().take(keyOffset.toInt)
    val readline =  raf.readLine()
    val keyString = readline.take(keyOffset.toInt)
    val dataString = readline.drop(keyOffset.toInt)
    (keyString, dataString)
  }

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] =
  {
//      val seq = for (i <- Range(st, ed)) yield getRecord(i)
//      seq.toVector
    val startNum :Int = 0
    val endNum : Int = 327680-1
    val startFileIndex : Int = st/numOfRecords
    val endFileIndex :Int = ed/numOfRecords
    val startRecordIndex : Int = st%numOfRecords
    val endRecordIndex : Int = ed%numOfRecords
    //val rangeFileIndex : (Int,Int) = (startFileIndex,endFileIndex)
    val keyOffset :Long = 10
    val valueOffset1 :Long = 32
    val valueOffset2 :Long = 52
    //val totalOffset :Long = keyOffset + 2 + valueOffset1 + 2 + valueOffset2
    val totalOffset :Long = 100

    def readFile (file : RandomAccessFile, stRecord: Int, edRecord: Int) : Vector[Record] ={
      var pos : Long =0
      val recordVector = for(i <- Range(stRecord,edRecord+1)) yield {
        pos = (totalOffset) * i
        file.seek(pos)
        val readline = file.readLine()
        val keyString = readline.take(keyOffset.toInt)
        val dataString = readline.drop(keyOffset.toInt)
        (keyString, dataString) : Record
      }
      recordVector.toVector

    }

    //case 1 => startFileIndex == endFileIndex
    if (startFileIndex == endFileIndex) {
      val raf = new RandomAccessFile(fileList(startFileIndex), "r")
      readFile(raf,startRecordIndex,endRecordIndex)
     }//case 2. startFileIndex+1 == endFileIndex
    else if (startFileIndex+1 == endFileIndex){
      val startRaf = new RandomAccessFile(fileList(startFileIndex), "r")
      val endRaf = new RandomAccessFile(fileList(endFileIndex), "r")
      readFile(startRaf,startFileIndex,endNum)++readFile(endRaf,startNum,endFileIndex)
    }// case 3. startFileIndex != endFileIndex
    else {
      val startRaf = new RandomAccessFile(fileList(startFileIndex), "r")
      val endRaf = new RandomAccessFile(fileList(endFileIndex), "r")
      //start+1 ~ end-1
      val vectorRaf: Vector[RandomAccessFile] = {
        //seq of RnadomAccessFIle
        val seq =  for (i <- Range(startFileIndex + 1, endFileIndex - 1)) yield {
          new RandomAccessFile(fileList(i), "r")
        }
        seq.toVector
    }
      val middleFilesEntireRecords :Vector[Record] = vectorRaf.flatMap(file => readFile(file,startNum,endNum))
      // st~endNum ++ ~ midlle whole file ++ startNum~ed
      readFile(startRaf,startFileIndex,endNum) ++ middleFilesEntireRecords ++ readFile(endRaf,startNum,endFileIndex)

    }


  }

  //It called in sorted file. if(key > sortedkey) return index of sortedkey (assume sorting is ascending)
  def getIndexofKey(key : String) :  Int = ???

}

class SingleFile(name : String) extends IBigFile {

  def numOfRecords: Int = ???

  // get i'th record
  def getRecord(i: Int): Record = {

    //define randomAccessFile just for read("r)
    val raf = new RandomAccessFile(name, "r")

    //set Offset for key or value
    //ex) AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111
    //    10 - 32 - 52
    val keyOffset :Long = 10
    val valueOffset1 :Long = 32
    val valueOffset2 :Long = 52
    //num 2 blank for each blank !?!?
    //val totalOffset :Long = keyOffset + 2 + valueOffset1 + 2 + valueOffset2
    val totalOffset :Long= 100
    //set position
    val pos :Long = (totalOffset) * i
    raf.seek(pos)

    //val keyVal = raf.readLine().take(keyOffset.toInt)
    val readline = raf.readLine()
    val keyString = readline.take(keyOffset.toInt)
    val dataString = readline.drop(keyOffset.toInt)
    (keyString, dataString)
  }

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] =
  {
//    val seq = for (i <- Range(st, ed)) yield getRecord(i)
//    seq.toVector

    val raf = new RandomAccessFile(name, "r")
    val keyOffset :Long = 10
    val valueOffset1 :Long = 32
    val valueOffset2 :Long = 52
    //num 2 blank for each blank !?!?
    val totalOffset :Long = keyOffset + 2 + valueOffset1 + 2 + valueOffset2
    var pos : Long =0
    val recordVector = for(i <- Range(st,ed)) yield {
      pos = (totalOffset) * i
      raf.seek(pos)
      val readline = raf.readLine()
      val keyString = readline.take(keyOffset.toInt)
      val dataString = readline.drop(keyOffset.toInt)
      (keyString, dataString) : Record
    }
    recordVector.toVector

  }

  def getIndexofKey(key : String) :  Int = ???
}

class ConstFile extends IBigFile{
  // returns total number of records in this file
  def numOfRecords: Int = 327680 * 2

  // get i'th record
  def getRecord(i: Int): Record = {
    val keyVal = 1000* 10000 - i
    val keyString = "%010d".format(keyVal)
    val dataString = "7" * 100
    (keyString, dataString)
  }
  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] =
  {
    val seq = for (i <- Range(st, ed)) yield getRecord(i)
    seq.toVector
  }


  def getIndexofKey(key : String) :  Int = ???

}

trait IOutputFile {
  def setRecords(records : Vector[Record]) : Future[Unit]
  def appendRecord(record: Record ) : Future[Unit]
  def toInputFile : IBigFile
}

class NullOutputFile extends IOutputFile {
  def setRecords(records : Vector[Record]) : Future[Unit] = Future{
  }
  def write(record: Record ) : Future[Unit] = Future {

  }
  def appendRecord(record: Record ) : Future[Unit] = ???
  def toInputFile : IBigFile = new ConstFile

}

  // Delete abstract keyword after implementing BigFile
class BigOutputFile(outputPath: String) extends IOutputFile {
//that has two case -> file is exist or non-exist.
  val randomAccessFile : RandomAccessFile= {
    new RandomAccessFile(new File(outputPath), "rw")
  }

    def setRecords(records: Vector[Record]): Future[Unit] = Future {

      //randomAccessFile.seek(randomAccessFile.length())

      val getBytesOfRecord : Int = 100
      val filelength = records.size * getBytesOfRecord
      val out : MappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, filelength)
        for (i <- Range(0, records.size)) {
          randomAccessFile.seek(randomAccessFile.length())
          val pair = records(i)
          val text = (pair._1 + pair._2 + "\n")
          val buf = ByteBuffer.wrap(text.getBytes)
          out.put(buf)
        }
      randomAccessFile.close()
      }



    def appendRecord(record: Record ) : Future[Unit] = Future {
      randomAccessFile.seek(randomAccessFile.length())
        //val existFileList = d.listFiles.filter(_.isFile).toList
        //val memoryMappedFile = new RandomAccessFile(existFileList(0), "rw")
        val getBytesOfRecord : Int = 100
        val filelength = getBytesOfRecord
        val out : MappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, filelength);
        val text = (record._1 + record._2 + "\n")
        val buf = ByteBuffer.wrap(text.getBytes)
        out.put(buf)
        randomAccessFile.close()
    }

    def toInputFile : IBigFile = {
      new SingleFile(outputPath)
    }
}

class BigOutputFile_temp(outputPath: String) extends IOutputFile {
  def setRecords(records: Vector[Record]): Future[Unit] = Future{
    println("setRecords : size="+ records.size)
    val memoryMappedFile = new RandomAccessFile("largeFile2.txt", "rw");
    //Mapping a file into memory
    val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, records.size * 112);
    println("setRecords 1")
    //Writing into Memory Mapped File
    for(i <- Range(0,records.size) )
    {
      val pair = records(i)
      val str = (pair._1 + " " + pair._2 + "\n")
      val buf = ByteBuffer.wrap(str.getBytes)
      out.put(buf)
    }
    memoryMappedFile.close()
    println("setRecords end")
  }
  def appendRecord(record: Record ) : Future[Unit] = Future{

    val str = (record._1 + " " + record._2 + "\n")
    val buf = ByteBuffer.wrap(str.getBytes)
  }
  def toInputFile : IBigFile = new ConstFile

}
