import java.net._
import java.io._
import scala.io._
import scala.util.control.Breaks._

package object master {
/*  class IA(val ipAddress : String) {
 *     def toIntList: List[Int] = ???
 * }
 */
  class Master {
    var ipAddrList : List[String] = Nil
    var slaveThread : List[Thread] = Nil
    val port : Int = 5959
    def myIp : String = InetAddress.getLocalHost().getHostAddress()
    def start(slaveNum : Int) {
      val sock = new ServerSocket(port)
      var acceptNum = 0
      println("Listening...")
      breakable {
        while (true) {
          if(acceptNum >= slaveNum) {sock.close(); break}
          val client = sock.accept
          acceptNum = acceptNum + 1
          println("Connected")
          val t = new Thread(new masterHandle(client, this))
          addSlaveThread(t)
          t.start()
        }
      }
    }
    def addIPList(ipaddr : String) {
      ipAddrList = (ipaddr::ipAddrList).sorted
    }
    def addSlaveThread(t : Thread) {
      slaveThread = t :: slaveThread
    }

  }
  
  class masterHandle(val cSocket : Socket, val master : Master) extends Runnable {
    def run()
    {
      this.synchronized {
      master.addIPList(cSocket.getRemoteSocketAddress().toString())
      }
      println("Hi!")
      cSocket.close()
    }
  }

}
