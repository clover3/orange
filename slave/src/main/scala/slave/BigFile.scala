package slave

/**
 * Created by Clover on 2015-10-30.
 */

import slave.Record._

import scala.concurrent.Future



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
    // generate BigFile from inputDirs
    // it reads all files in inputDirs and treat them as single file
    def numOfRecords: Int = ???

  // get i'th record
    def getRecord(i: Int): Record = ???

  // return collection of records
  // starting from st to ed  ( it should not include ed'th record )
    def getRecords(st: Int, ed: Int): Future[Vector[Record]] = ???
  }

  // Delete abstract keyword after implementing BigFile
  abstract class BigOutputFile(outputPath: String) extends IBigFile {
    def setRecords(records : Vector[Record]) : Future[Unit]
  }