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

import scala.concurrent.{Promise, Future}

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
  var inBigFile : List[(String, Promise[BigInputFile])] = Nil
  lazy val isWritable : List[(String,Boolean)] = {
    (for(i <- Range(0, slaveNum)) yield {
      (ipList(i), false)
    }).toList
  }
  def connect(sockChan : SocketChannel): Unit =
  {
    if(sockChan.isConnectionPending)
      println(" # connection is pending")
    sockChan.finishConnect()
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
  def write(sock : SocketChannel) : Unit =
  {
    sock.write(ByteBuffer.wrap("hi".getBytes()))
       /*
       inBigFile onSuccess {
       case Bigfile => while(Bigfile.position != Bigfile.send_capacity){
         BigFile -> buf // split buf size..
         buf -> sock.write(buf)
         Bigfile.position = Bigfile.position + size(buf)
          }
        }
        */
  }
}


object SlaveSock {
  var ip2Bigfile : Map[String, BigOutputFile] = Map.empty
  def apply(ips : List[String]) = new SlaveSock {
    val ipList = ips
    val slaveServerSock = new SlaveServerSock(ipList)
    val slaveClientSock = new SlaveClientSock(ipList)
    def ip2Bigfile : Map[String, BigOutputFile] = slaveServerSock.ip2inBigfile ++ slaveClientSock.ip2inBigfile
    def ip2Sock : Map[String, SocketChannel] = slaveServerSock.ip2Sock ++ slaveClientSock.ip2Sock
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
    def getSock(ip : String) : Future[SocketChannel] = Future {
      var check : SocketChannel = null
      while(check == null) {
        ip2Sock.get(ip) match {
          case Some(sock) => check = sock
          case None =>
        }
      }
      check
    }
    def sendData(ip:String, file: Future[BigInputFile], st:Int, ed: Int) = Future {
      getSock(ip) onSuccess {
        case sock : SocketChannel => write(sock)
      }
    }
  }
}

/* I need one serverSock.. (maybe..?) */
class SlaveServerSock(val ipList : List[String]) extends SlaveSock with Runnable {
  val serverChannel  = ServerSocketChannel.open()
  var ip2Sock : Map[String, SocketChannel] = Map.empty
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
          val channel = selected.channel()
          channel match {
            case sockChan :  SocketChannel =>
              connect(sockChan)
              ip2Sock = ip2Sock +(sockChan.getRemoteAddress.toString -> sockChan)
            case _ => throw new Exception("only socket can connect")
          }
        }
        else if(selected.isReadable){
          read(selected)
        }
      }
      it.remove()
    }
  }

  def accept(selectionKey : SelectionKey): Unit =
  {
    val channel = selectionKey.channel()
    channel match {
      case serverChan: ServerSocketChannel =>
        val clientSock = serverChan.accept()
        clientSock.configureBlocking(false)
        clientSock.register(selector,OP_CONNECT | OP_READ )
        println("one client accepted.")
      case _ => throw new Exception("only ServerSocket can accept")
    }
  }
}


/* I need many slaveSock... (maybe..?) */
class SlaveClientSock(val ipList : List[String]) extends SlaveSock with Runnable {
  lazy val ip2Sock : Map[String, SocketChannel] = ( for (ip <- clientList) yield
      {
        val addr = new InetSocketAddress(InetAddress.getByName(ip._1), ip._2)
        val sock = SocketChannel.open(addr)
        sock.configureBlocking(false)
        sock.register(selector, OP_CONNECT | OP_READ )
        (ip._1, sock)
      }).toMap
  def run() =
  {
    while(selector.select() > 0) {
      val it = selector.selectedKeys().iterator()
      while(it.hasNext){
        val selected = it.next
        if(selected.isConnectable){
          val channel = selected.channel()
          channel match {
            case sockChan : SocketChannel =>
              connect(sockChan)
            case _ => throw new Exception("only socket can connect")
          }
        }
        else if(selected.isReadable){
          read(selected)
        }
        it.remove()
      }
    }
  }
}





