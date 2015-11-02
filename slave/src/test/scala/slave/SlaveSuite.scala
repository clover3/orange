package slave

import common.typedef._
import org.scalatest.FunSuite

class SlaveSuite extends FunSuite {

  test("Slave Calculation Test"){
    lazy val sock = new SlaveSocket("127.0.0.1:1234")
    val inputDirs = List("inputdir1", "inputdir2")
    val outputDir = "outdir"
    val calculation = SlaveSampler(sock, inputDirs, outputDir)
    val s:Sample = calculation.getSamples
    assert(calculation.totalSampleKey == s._2.size)
  //  s.print
    val s2 = parseSampleBuffer(s.toBuffer)
  //  s2.print
    assert(s == s2)
  }

  test("Partition Test"){
    val p1= List(new Partition("192.168.1.1", "AsfAGHM5om", "~sHd0jDv6X"),
      new Partition("192.168.1.1", "AsfAGHM5om", "~sHd0jDv6X")
    )
    val buf = p1.toByteBuffer
    val p2 = parsePartitionBuffer(buf)

    assert(p1 == p2)
  }
}