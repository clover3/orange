
package slave

import slave.sorter.SlaveSorter

object Main {
  
  def main(args: Array[String]) = {
    val oIndex = args.indexOf("-O")
    val iIndex = args.indexOf("-I")
    val tIndex = args.indexOf("-T")
    if(oIndex == -1 || iIndex == -1 || tIndex == -1) throw new Exception("argument is strange")
    val inputDir : Array[String] = args.slice(iIndex+1, tIndex)
    val tempDir : String = args(tIndex+1)
    val slave = new Slave(args(0), inputDir.toList, args(oIndex + 1), tempDir)
    slave.run()
  }
}
