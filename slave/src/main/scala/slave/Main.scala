
package slave

import java.nio._

object Main {
  
  def main(args: Array[String]) = {
    val oIndex = args.indexOf("-O")
    val iIndex = args.indexOf("-I")
    if(oIndex == -1 || iIndex == -1) throw new Exception("argument is strange")
    val inputDir : Array[String] = args.slice(iIndex+1, oIndex) 
    val client = new Slave(args(0), args(oIndex + 1))
    val buf = client.sendAndRecvOnce(ByteBuffer.wrap("Test".getBytes()))
    

    println(buf.get(0))
    client.sock.close()

  }
}
