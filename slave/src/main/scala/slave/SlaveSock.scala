package slave

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels._
import java.nio.channels.SelectionKey._
import slave.Record._
import scala.concurrent.ExecutionContext.Implicits.global
import java.lang.Thread

import common.typedef._

import scala.concurrent.Future

/**
 * Created by Soyeon on 2015-10-31.
 */

/* ipList example : List("141.223.83.113", "127.0.0.1", "141.223.175.212", ...)
 *  it is extracted by partition list.. (because I need shared ipList...)
 *  */

trait SlaveSock {
  val ipList : List[String]
  lazy val slaveNum : Int = ipList.length
  var basePort : Int = 5000
  lazy val ipPortList : List[(String, Int)] = ipList.map { ip : String => {basePort = basePort + 1; (ip, basePort)}}
  val myIp : String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
  lazy val (serverList, cList) : (List[(String, Int)], List[(String, Int)]) = ipPortList.span( _ != myIp)
  lazy val myIpPort : (String, Int) = cList.head
  lazy val clientList = cList.tail
  var ip2inBigfile : Map[String, BigOutputFile] = Map.empty
  val buf : ByteBuffer = ByteBuffer.allocate(100 * 10000) // I don't know how many I can use buffer...
  val selector = Selector.open()
  def connect(selectionKey: SelectionKey): Unit =
  {
    val channel = selectionKey.channel()
    channel match {
      case sockChan : SocketChannel =>
        if(sockChan.isConnectionPending)
          println(" # connection is pending")
        sockChan.finishConnect()
      case _ => throw new Exception("only Socket can connect")
    }
  }
  def read(selectionKey: SelectionKey) : Unit =
  {
    val channel = selectionKey.channel()
    channel match {
      case sockChan : SocketChannel =>
        sockChan.read(buf)
        buf.flip()
        print(buf)
        /*
        val records = parseRecordBuffer(buf)
        val sockIp = sockChan.socket().getInetAddress.toString
        if(ip2inBigfile.contains(sockIp))
          ip2inBigfile(sockIp).append(records)
        else {
          val outBigfile = new BigOutputFile("outputdir")
          outBigfile.setRecords(records)
          ip2inBigfile = ip2inBigfile + (sockIp -> outBigfile)
        }
        */
        buf.clear()
      case _ => throw new Exception("only socket can read")
    }
  }
}

object SlaveSock {
  var ip2Bigfile : Map[String, BigOutputFile] = Map.empty
  def apply(ips : List[String]) = new SlaveSock {
    val ipList = ips
    val slaveServerSock = new SlaveServerSock(ipList)
    val slaveClientSock = new SlaveClientSock(ipList)
    def ip2Bigfile : Map[String, BigOutputFile] = slaveServerSock.ip2inBigfile ++ slaveClientSock.ip2inBigfile

    def recvData(ip : String) : Future[BigOutputFile] = Future {
      var check : BigOutputFile= null
      while (check == null) {
        ip2Bigfile.get(ip) match {
          case Some(bigFile) => check = bigFile
          case None =>
        }
      }
      check
    }
  }
}

/* I need one serverSock.. (maybe..?) */
class SlaveServerSock(val ipList : List[String]) extends SlaveSock with Runnable {
  val serverChannel  = ServerSocketChannel.open()
  def run(): Unit =
  {
    serverChannel.bind(new InetSocketAddress("localhost",myIpPort._2))
    serverChannel.configureBlocking(false)
    serverChannel.register(selector, OP_ACCEPT)
    while(selector.select > 0){
      val it = selector.selectedKeys().iterator()
      while(it.hasNext){
        val selected = it.next
        if(selected.isAcceptable){
          accept(selected)
        }
        else if(selected.isConnectable) {
          connect(selected)
        }
        /*else if(selected.isWritable){
          write(selected)
        }*/
        else if(selected.isReadable){
          read(selected)

        }
        it.remove()
      }
    }
  }




  def accept(selectionKey : SelectionKey): Unit =
  {
    val channel = selectionKey.channel()
    channel match {
      case serverChan: ServerSocketChannel =>
        val clientSock = serverChan.accept()
        clientSock.configureBlocking(false)
        clientSock.register(selector,OP_CONNECT | OP_READ | OP_WRITE)
        println("one client accepted.")
      case _ => throw new Exception("only ServerSocket can accept")
    }
  }
}


/* I need many slaveSock... (maybe..?) */
class SlaveClientSock(val ipList : List[String]) extends SlaveSock with Runnable {
  def run() =
  {
    val Sock2ip : Map[String, SocketChannel] = ( for (ip <- clientList) yield
      {
        val addr = new InetSocketAddress(InetAddress.getByName(ip._1), ip._2)
        val sock = SocketChannel.open(addr)
        sock.configureBlocking(false)
        sock.register(selector, OP_CONNECT | OP_READ | OP_WRITE)
        (ip._1, sock)
      }).toMap
    while(selector.select() > 0) {
      val it = selector.selectedKeys().iterator()
      while(it.hasNext){
        val selected = it.next
        if(selected.isConnectable){
          connect(selected)
        }
        else if(selected.isReadable){
          read(selected)
        }
        /*else if(selected.isWritable()){
          write(selected)
        }*/
        it.remove()
      }
    }
  }
}





