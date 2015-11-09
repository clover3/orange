package slave

import java.io.File

import org.scalatest.FunSuite
import slave.sorter._



// usage:

/* block of code to be profiled*/

/**
 * Created by Clover on 2015-11-10.
 */

class BigFileSuite extends FunSuite {
  def profile[R](code: => R, t: Long = System.currentTimeMillis()) = (code, System.currentTimeMillis() - t)

  implicit class FileMonads(f: File) {
    def check = if (f.exists) Some(f) else None //returns "Maybe" monad
    def remove = if (f.delete()) Some(f) else None //returns "Maybe" monad
  }

  def deleteIfExist(path :String) = {
    for {
      foundFile <- new File(path).check
      deletedFile <- foundFile.remove
    } yield deletedFile
  }

  test("check file read"){
    val input: IBigFile = new MultiFile(List("inputdir1", "inputdir2"))

    val cnt = 10 * 10000
    val (result, time) = profile {input.getRecords(0, cnt)}
    println("getRecords - time elapsed(ms) : " + time )
    val (rCnt,time2) = profile{ result.size }
    println("vector.size - time elapsed(ms) : " + time2 )
    assert(rCnt == cnt)
    println("Keys : ")
    for(i <- Range(0,10) ) println(result(i)._1)
  }

  test("File write - few record using appendRecord"){
    val fileName = "out_test"
    deleteIfExist(fileName)
    val output : BigOutputFile = new BigOutputFile(fileName)

    val constFile = new ConstFile
    val records = constFile.getRecords(0,10)

    for( rec <- records ) output.appendRecord(rec)
    val readRecords = output.toInputFile.getRecords(0,10)
    assert(records == readRecords)
  }

  test("File write - many record - appendRecord") {
    val fileName = "out_test1"
    deleteIfExist(fileName)
    val output: BigOutputFile = new BigOutputFile(fileName)

    val constFile = new ConstFile
    val numOfRecord = 10 * 10000
    val (records, time) = profile {
      constFile.getRecords(0, numOfRecord)
    }
    println("record generation - time elapsed(ms) : " + time)

    val (_, time2) = profile {
      for (rec <- records) output.appendRecord(rec)
    }
    println("writing using (for+appendRecord) - time elapsed(ms) : " + time)
  }

  test("File write - many record - setRecords") {
    val fileName = "out_test1"
    deleteIfExist(fileName)
    val output: BigOutputFile = new BigOutputFile(fileName)

    val constFile = new ConstFile
    val numOfRecord = 10 * 10000
    val (records, time) = profile {
      constFile.getRecords(0, numOfRecord)
    }
    println("record generation - time elapsed(ms) : " + time)

    val (_, time2) = profile { output.setRecords(records) }
    println("writing using (setRecords) - time elapsed(ms) : " + time2)
  }
}