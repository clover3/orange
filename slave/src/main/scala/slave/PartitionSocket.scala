package slave

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

package socket{

class PartitionSocket(val master : String)  {
  val (masterIPAddr, masterPort) : (String, Int) = {
    val ipR = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):([0-9]+)""".r
    master match {
      case ipR(ip1,ip2,ip3,ip4,port) => (List(ip1, ip2, ip3, ip4).mkString("."), port.toInt)
      case _ => throw new Exception("IP error")
    }
  }
  val sock = SocketChannel.open(new InetSocketAddress(masterIPAddr, masterPort))

  def sendAndRecvOnce(buffer : ByteBuffer) : ByteBuffer = {
    var nbyte = 0
    nbyte = sock.write(buffer)
    buffer.clear()
    sock.read(buffer)
    buffer
  }
}
}