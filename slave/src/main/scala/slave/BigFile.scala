package slave

/**
 * Created by Clover on 2015-10-30.
 */

import java.io.{FileNotFoundException, File}

import common.typedef._
import slave.Record._

import scala.concurrent.Future
import scala.io.Source


trait IBigFile {
    // returns total number of records in this file
    def numOfRecords: Int

    // get i'th record
    def getRecord(i: Int): Record

    // return collection of records
    // starting from st to ed  ( it should not include ed'th record )
    def getRecords(st: Int, ed: Int): Future[Vector[Record]]

}

// Delete abstract keyword after implementing BigFile
class BigInputFile(inputDirs : List[String]) extends IBigFile {

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

    // generate BigFile from inputDirs
    // it reads all files in inputDirs and treat them as single file
  def numOfRecords: Int = {
      //fileList.map(numOfRecordsEachFile).foldLeft(0)(_+_)
      fileList.map(numOfRecordsEachFile).sum //(fileList.map(numOfRecordsEachFile):List[Int]).sum

      def numOfRecordsEachFile(file: File) :Int = {
        var sum :Int =0 ///sry for using var...T_T
        for (line <- Source.fromFile(file).getLines()) {
          sum = sum +1
        }
        sum
      }

//      def numOfRecordsEachFile(file: File) :Int = {
//        val sum = for (line <- Source.fromFile(file).getLines()) yield {
//        } sum
//      }
    1 // just for compile..
    }

  // get i'th record
  def getRecord(i: Int): Record = ???

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
  def getRecords(st: Int, ed: Int): Future[Vector[Record]] = {
    // 1.read file from inputDir -> 2.make fileList ->3. get Record -> 4.make Records

    //     // var ListRecordEachFile : vector[Record]
    //      def vectorRecordEachFile(file: File) : Vector[Record]  = {
    //       for (line <- Source.fromFile(file).getLines()) yield {
    //         new Record(line.take(10), line.drop(10)) // parsing File as " "
    //       }
    //       vectorRecordEachFile(file).toVector
    //     }
    //
    ////all file vector
    //    val vectorRecordAllFile : Vector[Record] = fileList.flatMap(file=>vectorRecordEachFile(file)).toVector
    //
    //    // from start index to end index
    //    val indexedVectorRecord : Vector[Record] = vectorRecordAllFile.slice(st,ed)
    //
    //  }
    ??? // just for compile...
  }


}

  // Delete abstract keyword after implementing BigFile
 abstract class BigOutputFile(outputPath: String) extends IBigFile {
    def setRecords(records : Vector[Record]) : Future[Unit]

  }