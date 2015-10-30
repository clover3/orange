package slave

/**
 * Created by Clover on 2015-10-30.
 */

import slave.Record._

  trait IFile;

  trait IBigFile {
    // returns total number of records in this file
    def numOfRecords: Int

    // get i'th record
    def getRecord(i: Int): Record

    // return collection of records
    // starting from st to ed  ( it should not include ed'th record )
    def getRecords(st: Int, ed: Int): Vector[Record]

  }

  // Delete abstract keyword after implementing BigFile
  abstract class BigInputFile extends IBigFile {

    // generate BigFile from inputDirs
    // it reads all files in inputDirs and treat them as single file
    def apply(inputDirs : List[String]) : IBigFile = ???
  }

  // Delete abstract keyword after implementing BigFile
  abstract class BigOutputFile extends IBigFile {
    def setRecords(records : Vector[Record])
    // generate BigFile to save outputFile
    // it makes files inside outputDir, and the file names starts with namePrefix
    // if outputDir = "Out" , namePrefix = "file",
    // it will generates "Out/file1", "Out/file2"... etc
    def apply(outputDir : String, namePrefix : String ) : IBigFile = ???
  }
