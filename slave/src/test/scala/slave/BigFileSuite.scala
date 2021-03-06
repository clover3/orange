package slave

import org.scalatest.FunSuite
import slave.Record._
import slave.util._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
// usage:

/* block of code to be profiled*/

/**
 * Created by Clover on 2015-11-10.
 */

class BigFileSuite extends FunSuite {


  test("check file read"){
    val input: IBigFile = new MultiFile(List("inputdir1", "inputdir2"))

    val cnt = 10 * 10000
    val (result, time) = profile {input.getRecords(0, cnt)}
    println("getRecords - time elapsed(ms) : " + time )
    val rCnt = result.size
    assert(rCnt == cnt)
    println("Keys : ")
    for(i <- Range(0,10) ) println(new String(result(i)._1))
  }

  test("check file read (ConstFile)"){
    val input: IBigFile = new ConstFile

    val cnt = 100 * 10000
    val (result, time) = profile {input.getRecords(0, cnt)}
    println("getRecords - time elapsed(ms) : " + time )
    val (rCnt,time2) = profile{ result.size }
    println("vector.size - time elapsed(ms) : " + time2 )
    assert(rCnt == cnt)
    println("Keys : ")
    for(i <- Range(0,10) ) println(new String(result(i)._1))
  }

  test("Record Comparison"){
    val input: IBigFile = new ConstFile
    val rec1 = input.getRecord(10)
    val rec2 = input.getRecord(20)
    val rec3 = input.getRecord(30)

    println(rec1.toStr)
    println(rec2.toStr)
    println(rec3.toStr)

    assert( rec1._1 > rec2._1)
    assert( rec1._1 >= rec2._1)
    assert( rec3._1 <= rec2._1 )
    assert( rec1._1 > rec3._1 )
    assert( (rec1._1).myEqual( rec1._1) == true )
  }

  test("Test for Prefetched read"){
    val fileName = "out_test"
    deleteIfExist(fileName)
    val output : BigOutputFile = new BigOutputFile(fileName)

    val constFile = new ConstFile
    val cn = 1000000
    val records = constFile.getRecords(0,cn)
    println("record size :" + records.size)
    assert(records(0)._1.length == 10)
    assert(records(0)._2.length == 90)
    for( rec <- records ) output.appendRecord(rec)
    output.close()
    val readRecords = output.toInputFile.getRecords(0,cn)

    def randomAccess(file:IBigFile, ways:Int, length:Int) = {
      val waylen = length / ways
      for( i <- Range(0,ways) ){
        for( j <- Range(0,waylen))
          {
            val s = file.getRecord(i*waylen+j)
          }
      }
    }

    val singleFile : IBigFile= output.toInputFilePreFetch
    val (result1, time) = profile { randomAccess(singleFile, 100, cn)  }
    println("singleFile modified getRecordCache time(ms):" + time)
    val record0= singleFile.getRecord(0)

    //val s2 = singleFile.getRecord(10080)
    //println("singleFile getrecord(0) :" + record0._1)
    //println("singleFile getrecord(10000) :" + s2._1)

    val singleFile2 :IBigFile = output.toInputFile
    val (result2, time2) = profile { randomAccess(singleFile2, 100, cn)  }
    println("singleFile origin getRecordCache time(ms): " + time2 )
    //val s1 = singleFile2.getRecord(10080)
    //println("singleFile getrecord(10000) :" + s1._1)
    //val s = singleFile.getRecord(1)
    //println("singleFile getrecord(1) :" + s._1)
  }

  test("File write - few record using appendRecord"){
    val fileName = "out_test"
    deleteIfExist(fileName)
    val output : BigOutputFile = new BigOutputFile(fileName)

    val constFile = new ConstFile
    val records = constFile.getRecords(0,10)
    println("record size :" + records.size)
    assert(records(0)._1.length == 10)
    assert(records(0)._2.length == 90)
    for( rec <- records ) {
      output.appendRecord(rec)
    }
    output.close()
    val readRecords = output.toInputFile.getRecords(0,10)
    println(readRecords(0)._1)
    println(readRecords(0)._2)
    println(readRecords(1)._1)
    println(readRecords(1)._2)
    assert(readRecords(0)._1.length == 10)
    assert(readRecords(0)._2.length == 90)
    assert(records == readRecords)
    val singleFile : IBigFile= output.toInputFile
    //val s1 = output.toInputFile.getRecordDirect(i)
    val s = singleFile.getRecord(1)
    println("singleFile getrecord(1) :" + s._1)


  }


  test("File write - many record - appendRecord") {
    val fileName = "out_test1"
    deleteIfExist(fileName)
    val output: IOutputFile = new BigOutputFile(fileName)

    val constFile = new ConstFile
    val numOfRecord = 100 * 10000
    val (records, time) = profile {
      constFile.getRecords(0, numOfRecord)
    }
    println("record generation - time elapsed(ms) : " + time)

    val (_, time2) = profile {
      var i = 0
      while( i < records.size ){
      //for (rec <- records) {
        val rec = records(i)
        output.appendRecord(rec)
        i = i + 1
      }
      output.close()
    }
    println("writing using (for+appendRecord) - time elapsed(ms) : " + time2)
  }

  test("File write - many record - setRecords") {
    val fileName = "out_test1"
    deleteIfExist(fileName)
    val output: IOutputFile = new BigOutputFile(fileName)

    val constFile = new ConstFile
    val numOfRecord = 100 * 10000
    val (records, time) = profile {
      constFile.getRecords(0, numOfRecord)
    }
    println("record generation - time elapsed(ms) : " + time)

    val (_, time2) = profile {
      Await.result(output.setRecords(records), Duration.Inf)
    }
    println("writing using (setRecords) - time elapsed(ms) : " + time2)
  }

  test("read-write test"){
    val input: IBigFile = new MultiFile(List("inputdir_one"))
    val outpath = "out_test1"

    val cnt = input.numOfRecords
    val (records, time) = profile {input.getRecords(0, cnt)}
    println("getRecords - time elapsed(ms) : " + time )

    assert(records(0)._1.length == 10)
    assert(records(0)._2.length == 90)

    val rCnt = records.size
    assert(rCnt == cnt)

    val output: IOutputFile = new BigOutputFile(outpath)
    val (_, time2) = profile {
      var i = 0
      while( i < records.size ){
        //for (rec <- records) {
        val rec = records(i)
        output.appendRecord(rec)
        i = i + 1
      }
      output.close()
    }
    println("appendRecord - time elapsed(ms) : " + time2)
  }

  test("output empty file"){
    val outpath = "empty_out"
    val output: IOutputFile = new BigOutputFile(outpath)
    output.close()
    output.toInputFile
  }

  test("partial file test") {
    val file = new ConstFile
    val vfile = new PartialFile(file, 10, 15)

    assert(vfile.numOfRecords == 5)

    val rec :BRecord = vfile.getRecord(0)
    println("rec(0):" + rec.toStr)
    val recs = vfile.getRecords(0, vfile.numOfRecords)
    println("recs(0:5):")
    recs.map(r => println(r.toStr))
  }


}

