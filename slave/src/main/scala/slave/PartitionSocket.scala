package slave

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import scala.concurrent.ExecutionContext.Implicits.global

package socket {

import scala.concurrent.{Future, Promise}
import scala.util.Success

  class PartitionSocket(val master: String) {
    val (masterIPAddr, masterPort): (String, Int) = {
      val ipR = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):([0-9]+)""".r
      master match {
        case ipR(ip1, ip2, ip3, ip4, port) => (List(ip1, ip2, ip3, ip4).mkString("."), port.toInt)
        case _ => throw new Exception("IP error")
      }
    }
    val sock = SocketChannel.open(new InetSocketAddress(masterIPAddr, masterPort))

    def sendAndRecvOnce(buffer: ByteBuffer): ByteBuffer = {
      var nbyte = 0
      nbyte = sock.write(buffer)
      buffer.clear()
      sock.read(buffer)
      buffer
    }

    def checkMasterRequest(ip: String, ok: Promise[Unit]): Unit = {
      val buffer = ByteBuffer.allocate(15)
      buffer.put(ip.getBytes())
      sock.write(buffer)
      buffer.clear()
      sock.read(buffer)
      val s = buffer.get(0).toString
      if (s == "83") {
        ok.complete(Success(()))
      }
      else {
        checkMasterRequest(ip, ok)
      }
    }

    def sendfinish(f: Future[Unit]): Unit = {
      f onSuccess {
        case u =>
          val buffer = ByteBuffer.allocate(2)
          buffer.put("FN".getBytes())
          sock.write(buffer)
      }
    }

    def sendok(p : Promise[Unit]): Unit = {
      val buffer = ByteBuffer.allocate(2)
      buffer.put("OK".getBytes())
      sock.write(buffer)
      p.complete(Success(()))
    }
  }
}
