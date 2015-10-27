package slave

import common.typedef._
import org.scalatest.FunSuite

class SlaveSuite extends FunSuite {

  test("Slave Calculation Test"){
    lazy val sock = new SlaveSocket("127.0.0.1:1234")
    val inputDirs = List("inputdir1", "inputdir2")
    val outputDir = "outdir"
    val calculation = SlaveCalculation(sock, inputDirs, outputDir)
    val s:Sample = calculation.getSamples
    assert(calculation.totalSampleKey == s._2.size)
    s.print
    s.toBuffer

  }
}