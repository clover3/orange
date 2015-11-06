package slave

/**
 * Created by Clover on 2015-10-30.
 */

import java.io.{File, FileNotFoundException, RandomAccessFile}
import java.nio.ByteBuffer
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

}


// I add argument inputDirs(:List[String])
class BigInputFile(inputDirs : List[String])  extends IBigFile{

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
    val totalOffset :Long = keyOffset + 2 + valueOffset1 + 2 + valueOffset2

    //set position
    val pos :Long = (totalOffset) * recordIndex
    raf.seek(pos)

    //val keyVal = raf.readLine().take(keyOffset.toInt)
    val keyString = raf.readLine().take(keyOffset.toInt)
    val dataString = raf.readLine().drop(keyOffset.toInt)
    (keyString, dataString)
  }

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Vector[Record] =
  {
  
      val seq = for (i <- Range(st, ed)) yield getRecord(i)
      seq.toVector
    
  }

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

}

trait IOutputFile {
  def setRecords(records : Vector[Record]) : Future[Unit]
  def write(record: Record ) : Future[Unit]
  def toInputFile : IBigFile
}

class NullOutputFile extends IOutputFile {
  def setRecords(records : Vector[Record]) : Future[Unit] = Future{
  }
  def write(record: Record ) : Future[Unit] = Future {

  }

  def toInputFile : IBigFile = new ConstFile

}

  // Delete abstract keyword after implementing BigFile
class BigOutputFile(outputPath: String) extends IOutputFile {
//that has two case -> file is exist or non-exist.

    def setRecords(records: Vector[Record]): Future[Unit] = Future {
      val d = new File(outputPath)
      if (d.exists && d.isDirectory) {
        val existFileList = d.listFiles.filter(_.isFile).toList

        //maybe one file...
        val memoryMappedFile = new RandomAccessFile(existFileList(0), "rw")
        //maybe  memoryMappedFile.lengrth?
        val filelength = records.size * 112
        val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, filelength);
        for (i <- Range(0, records.size)) {
          val pair = records(i)
          val text = (pair._1 + " " + pair._2 + "\n")
          val buf = ByteBuffer.wrap(text.getBytes)
          out.put(buf)
        }
        memoryMappedFile.close()
      }

      else {
        val memoryMappedFile = new RandomAccessFile("outputFile.txt", "rw")
        val filelength = records.size * 112
        val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, filelength);

        for (i <- Range(0, records.size)) {
          val pair = records(i)
          val text = (pair._1 + " " + pair._2 + "\n")
          val buf = ByteBuffer.wrap(text.getBytes)
          out.put(buf)
        }

        memoryMappedFile.close()
      }

    }
    def write(record: Record ) : Future[Unit] = Future {
      val d = new File(outputPath)
      if (d.exists && d.isDirectory) {
        val existFileList = d.listFiles.filter(_.isFile).toList
        val memoryMappedFile = new RandomAccessFile(existFileList(0), "rw")
        val filelength = 112
        val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, filelength);
        val text = (record._1 + " " + record._2 + "\n")
        val buf = ByteBuffer.wrap(text.getBytes)
        out.put(buf)
        memoryMappedFile.close()
      }
      else{
        val memoryMappedFile = new RandomAccessFile("outputFile.txt", "rw")
        val filelength =  112
        val out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, filelength);
        val text = (record._1 + " " + record._2 + "\n")
        val buf = ByteBuffer.wrap(text.getBytes)
        out.put(buf)
        memoryMappedFile.close()

      }

    }
    def toInputFile : IBigFile = new ConstFile
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
  def write(record: Record ) : Future[Unit] = Future{

    val str = (record._1 + " " + record._2 + "\n")
    val buf = ByteBuffer.wrap(str.getBytes)
  }
  def toInputFile : IBigFile = new ConstFile

}
