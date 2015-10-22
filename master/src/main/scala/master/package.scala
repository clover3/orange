import java.net._
import java.io._
import scala.io._
import scala.util.control.Breaks._
import scala.util.Sorting
import Array._

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

  //merging input data from each slaves
//  implicit class MergingInputData(val threads){
//
//  }


  //sorting the merged input data from buffer
  implicit class SortingKey(val d : Array[String]) {

    def QuickSort(a: Array[String], first: Int, last: Int) {
      var f: Int = first
      // f and i are index
      var l: Int = last
      var pivot: String = ""

      if (last - first > 0) {
        //quicksort
        pivot = a(f)
        while (l > f) {
          while ((a(f) compareTo pivot) <=0 && f <= last && l > f) {
            f += 1
          }
          while ((a(l) compareTo pivot) >0 && l >= first && l >= f) {
            l -= 1
          }
          if (l > f) {
            swap(a, l, f)
          }
        }
      }
    }

      def swap(array: Array[String], a: Int, b: Int){
      var tmp: String = array(a)
      array(a) = array(b)
      array(b) = tmp
    }

    QuickSort(d,0,100) //example 100;

  }


  type slaveID = Int

  class Master {
    var ipAddrList : List[String] = Nil
    var slaveThread : List[Thread] = Nil
    var slaveSocket : Map[slaveID, Socket] = Map.empty
    var id2IP : Map[slaveID, String] = Map.empty
    val port : Int = 5959
    def myIp : String = InetAddress.getLocalHost().getHostAddress()
    def start(slaveNum : Int) {
      val sock = new ServerSocket(port)
      var acceptNum = 0
//      val s :String = "asd"
//      val d :String = "asdf"
//      println(s.compareTo(d))
      println("Listening...")
      breakable {
        while (true) {
          if(acceptNum >= slaveNum) {sock.close(); break}
          val client = sock.accept
          acceptNum = acceptNum + 1
          println("Connected")
          slaveSocket = slaveSocket + (acceptNum -> client)
          id2IP = id2IP + (acceptNum -> client.getRemoteSocketAddress().toString().toIPList.toIPString)
          val t = new Thread(new masterHandle(client, this))
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
