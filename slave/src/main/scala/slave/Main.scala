
package slave

import java.nio._

object Main {
  
  def main(args: Array[String]) = {

    val client = new Slave("127.0.0.1:5959", "/data")
    val buf = client.sendAndRecvOnce(ByteBuffer.wrap("Test".getBytes()))
    

    println(buf.get(0))
    client.sock.close()

  }
}
