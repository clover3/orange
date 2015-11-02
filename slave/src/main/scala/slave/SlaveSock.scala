package slave

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels._
import java.nio.channels.SelectionKey._
import java.lang.Thread

import common.typedef._

/**
 * Created by Soyeon on 2015-10-31.
 */

/* ipList example : List("141.223.83.113", "127.0.0.1", "141.223.175.212", ...)
 *  it is extracted by partition list.. (because I need shared ipList...)
 *  */

trait SlaveSock extends Runnable {
  val ipList : List[String]
  val slaveNum : Int = ipList.length
  var basePort : Int = 5000
  val ipPortList : List[(String, Int)] = ipList.map { ip : String => {basePort = basePort + 1; (ip, basePort)}}
  val myIp : String = InetAddress.getLocalHost().getHostAddress().toIPList.toIPString
  val (serverList, cList) : (List[(String, Int)], List[(String, Int)]) = ipPortList.span( _ != myIp)
  val myIpPort : (String, Int) = cList.head
  val clientList = cList.tail
}

/* I need one serverSock.. (maybe..?) */
class SlaveServerSock(val ipList : List[String]) extends SlaveSock {
  val serverChannel = ServerSocketChannel.open()
  val selector = Selector.open()
  def run(): Unit =
  {
    serverChannel.bind(new InetSocketAddress("localhost",myIpPort._2))
    serverChannel.configureBlocking(false)
    serverChannel.register(selector, OP_ACCEPT)


  }
}


/* I need many slaveSock... (maybe..?) */
class SlaveClientSock(val ipList : List[String]) extends SlaveSock {
  def run() =
  {

  }
}



