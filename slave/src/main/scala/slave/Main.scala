
package slave

import slave.sorter.SlaveSorter

object Main {
  
  def main(args: Array[String]) = {
    val oIndex = args.indexOf("-O")
    val iIndex = args.indexOf("-I")
    if(oIndex == -1 || iIndex == -1) throw new Exception("argument is strange")
    val inputDir : Array[String] = args.slice(iIndex+1, oIndex) 
    val slave = new Slave(args(0), inputDir.toList, args(oIndex + 1))
    slave.run()
  }
}
