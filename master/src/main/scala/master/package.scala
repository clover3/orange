import java.net._
import java.io._
import java.nio.channels._
import scala.io._
import scala.util.control.Breaks._

package object master {
/*  class IA(val ipAddress : String) {
 *     def toIntList: List[Int] = ???
 * }
 */

  implicit class StringCompanionOps(val s: String) extends AnyVal {
    def toIPList : List[Int] = {
      val R = "/(.*):[0-9]+".r
      val R2 = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})""".r
      s match {
        case R(ip) => {ip.split('.').map(_.toInt).toList}
        case R2(ip1,ip2,ip3,ip4) => List(ip1.toInt, ip2.toInt, ip3.toInt, ip4.toInt)
        case _ => {throw new Exception("IP error")}
      }
    }
  }
  
  implicit class ListCompanionOps(val l: List[Int]) extends AnyVal {
    def toIPString : String = {l.map{_.toString}.mkString(".")}
  }

  type slaveID = Int

  object Master {
    var ipAddrList : List[String] = Nil
    var slaveThread : List[Thread] = Nil
    var id2Slave : Map[slaveID, Slave] = Map.empty
    val port : Int = 5959
    def myIp : String = InetAddress.getLocalHost().getHostAddress()
    def start(slaveNum : Int) {
      val server = ServerSocketChannel.open()
      val sock = server.socket()
      sock.bind(new InetSocketAddress(port))


      var acceptNum = 0
      println("Listening...")
      breakable {
        while (true) {
          if(acceptNum >= slaveNum) {sock.close(); break}
          val client = sock.accept()
          acceptNum = acceptNum + 1
          println("Connected")
          addIPList(client.getRemoteSocketAddress().toString())
          val slave = new Slave(acceptNum, client, client.getRemoteSocketAddress().toString().toIPList.toIPString)
          id2Slave = id2Slave + (acceptNum -> slave)
          val t = new Thread(slave)
          addSlaveThread(t)
          t.start()
        }
      }
    }
    def addIPList(ipaddr : String) {
      object IPOrdering extends Ordering[List[Int]] {
        def compare(a : List[Int], b:List[Int]) = a.head compare b.head
      }
      ipAddrList = (ipaddr::ipAddrList).map(_.toIPList).sorted(IPOrdering).map(_.toIPString)
    }
    def addSlaveThread(t : Thread) {
      slaveThread = t :: slaveThread
    }

  }
  

  class Slave (val id : slaveID, val sock : Socket, val ip : String) extends Runnable {
    def givePartition(buffer : ByteBuffer) = {
      buffer.clear()
      while(sock.read(buffer) != -1) {
// input buffer handler(consider partition range...?)
// and consider write buffer content..

////////////////////////////////////////////////////
      sock.write(buffer)
      
      }
    }
                                                                                                                                                                                                                                                                                                               
    def run()
    {
//      this.synchronized {
//      master.addIPList(cSocket.getRemoteSocketAddress().toString())
//      }
      println("Hi!")
// just example!  I don't know buffer capacity uuu..
      val inOutBuffer = ByteBuffer.allocatedDirect(1024)
      givePartition(inOutBuffer)

      sock.close()
    }
  }


}
