
package slave

import java.nio._

object Main {
  
  def main(args: Array[String]) = {

    val client = new Slave("127.0.0.1:5959", "/data")
    client.sendOnce(ByteBuffer.wrap("Test".getBytes()))
    val buf = ByteBuffer.allocate(1024)
    
    client.sock.read(buf)

    println(buf.get(0))
    client.sock.close()

  }
}
