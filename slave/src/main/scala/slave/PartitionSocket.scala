package slave

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import scala.concurrent.ExecutionContext.Implicits.global

package socket {

import scala.concurrent.{Future, Promise}
import scala.util.Success
import scala.annotation.tailrec

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
    def checkMasterRequest(ip: String, ipP : Promise[String]): Unit = {
      val buffer = ByteBuffer.allocate(16)
      println(ip)
      buffer.put((ip + "\n").getBytes())
      println(buffer)
      buffer.flip()
      sock.write(buffer)
      print("checkMasterRequest write")
      buffer.flip()
      val nbytes = sock.read(buffer)
      buffer.flip()
      val bytea : Array[Byte] = new Array(nbytes)
      buffer.get(bytea)
      val s = new String(bytea, "ASCII").trim()
      print("checkMasterRequest : " + s)
      ipP.complete(Success(s))
    }

    def sendfinish(f: Future[Unit], endp : Promise[Unit]): Unit = {
      print(f.isCompleted)
      
      f onSuccess {
        case u =>
          println("sendFN start")
          val buffer = ByteBuffer.allocate(3)
          buffer.put("FN\n".getBytes())
          buffer.flip()
          sock.write(buffer)
          endp.complete(Success(()))
          println("send FN")
      }
      
    }

    def sendok(p : Promise[Unit]): Unit = {
      val buffer = ByteBuffer.allocate(3)
      buffer.put("OK\n".getBytes())
      buffer.flip()
      sock.write(buffer)
      p.complete(Success(()))
      println("send OK")
    }
  }
}
