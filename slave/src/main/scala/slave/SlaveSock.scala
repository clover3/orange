package slave

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey._
import java.util
import io.netty.bootstrap.{ServerBootstrap, Bootstrap}
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.socket._
import io.netty.channel.nio.{NioEventLoopGroup, NioEventLoop}
import io.netty.channel.socket.nio.{NioSocketChannel, NioServerSocketChannel}
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.commons.logging.LogFactory
import slave.Record._
import scala.concurrent.ExecutionContext.Implicits.global
import java.lang.Thread

import common.typedef._

import scala.concurrent.{Promise, Future}
import scala.concurrent.Await
import scala.concurrent.duration._
/**
 * Created by Soyeon on 2015-10-31.
 */

/* ipList example : List("141.223.83.113", "127.0.0.1", "141.223.175.212", ...)
 *  it is extracted by partition list.. (because I need shared ipList...)
 *  */


// given IP and bytes, write downs data into IBigFile
class ByteConsumer{
  var ip2inBigfile : Map[String, BigOutputFile] = Map.empty
  var inBigFile : List[(String, Promise[BigInputFile])] = Nil

  def read(sockIp : String, records : Vector[Record]) : Unit =
  {
    println("read ### ")

    if(ip2inBigfile.contains(sockIp))
      ip2inBigfile(sockIp).setRecords(records)
    else {
      val outBigfile = new BigOutputFile("outputdir")
      outBigfile.setRecords(records)
      ip2inBigfile = ip2inBigfile + (sockIp -> outBigfile)
    }

  }
  def get2Map() : Map[String,BigOutputFile] = ip2inBigfile
}


trait SlaveSock {
  val ipList : List[String]
  lazy val slaveNum : Int = ipList.length
  var basePort : Int = 5000
  lazy val ipPortList : List[(String, Int)] = ipList.map { ip : String => {basePort = basePort + 1; (ip, basePort)}}
  val myIp : String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
  /*val myIp = "127.0.0.1"*/
  lazy val (serverList, cList) : (List[(String, Int)], List[(String, Int)]) = ipPortList.span( _._1 != myIp)
  lazy val myIpPort : (String, Int) = cList.head
  lazy val clientList = cList.tail

  val buf : ByteBuffer = ByteBuffer.allocate(100 * 10000) // I don't know how many I can use buffer...

  def write(sock : Channel) : Unit =
  {
    println("write hi")
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
    slaveServerSock.start()
    val slaveClientSock = new SlaveClientSock(ipList)
    slaveClientSock.start()
    def ip2Bigfile : Map[String, BigOutputFile] = slaveServerSock.byteConsumer.get2Map() ++ slaveClientSock.byteConsumer.get2Map()
    def ip2Sock : Map[String, Channel] = slaveServerSock.ip2Sock ++ slaveClientSock.ip2Sock
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
    def getSock(ip : String) : Future[Channel] = Future {
      var check : Channel = null
      while(check == null) {
        ip2Sock.get(ip) match {
          case Some(sock) => check = sock
            println("getSock")
          case None =>
        }
      }
      check
    }
    def sendData(ip:String, file: Future[BigInputFile], st:Int, ed: Int) = {
      println(ip)
      println(ip2Sock)
      val sock = Await.result (getSock(ip), Duration.Inf)
      write(sock)
      println(serverList)
      println(clientList)
    }
  }
}

class Buf2VectorRecordDecode extends ByteToMessageDecoder {
  override def decode(channelHandlerContext: ChannelHandlerContext, byteBuf: ByteBuf, list: util.List[AnyRef]): Unit = {
    if(byteBuf.readableBytes() < 100) {
      return;
    }
    val recordnum: Int = byteBuf.readableBytes() / 100
    val result : Vector[Record] = (for (i <- Range(0, recordnum))
    yield {
      (new String(byteBuf.readBytes(10).array()), new String(byteBuf.readBytes(90).array()))
    }).toVector
    list.add(result)
  }
}

class ServerHandler(ip:String, byteconsumer : ByteConsumer) extends ChannelInboundHandlerAdapter {
  override def channelRead(channelHandlerContext: ChannelHandlerContext, msg : Object) = {
    val vectorRecord : Vector[Record] = msg.asInstanceOf[Vector[Record]]
    byteconsumer.read(ip, vectorRecord)
  }
  override def channelReadComplete(channelHandlerContext: ChannelHandlerContext): Unit = {

  }
  override def exceptionCaught(channelHandlerContext: ChannelHandlerContext, cause : Throwable): Unit = {
    cause.printStackTrace()
    channelHandlerContext.close()
  }
}

/* I need one serverSock.. (maybe..?) */
class SlaveServerSock(val ipList : List[String]) extends SlaveSock {
  val LOG = LogFactory.getLog(classOf[SlaveServerSock].getName)
  var ip2Sock : Map[String, SocketChannel] = Map.empty
  val parentGroup = new NioEventLoopGroup(1)
  val childGroup = new NioEventLoopGroup()
  val byteConsumer = new ByteConsumer ()
  def start(): Unit =
  {
    if(!serverList.isEmpty){
      try {
        val hostname = InetAddress.getByName(myIpPort._1)
        val port = myIpPort._2
        val bs: ServerBootstrap = new ServerBootstrap()
        bs.group(parentGroup, childGroup)
          .channel(classOf[NioServerSocketChannel])
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new ChannelInitializer[SocketChannel] {
            override def initChannel(c: SocketChannel): Unit = {
              LOG.info("ConnectionReceiver accepted : " + c.remoteAddress())
              ip2Sock = ip2Sock + (c.remoteAddress().toString.toIPList.toIPString -> c)
              LOG.info(ip2Sock)
              val cp: ChannelPipeline = c.pipeline
              cp.addLast(new Buf2VectorRecordDecode())
              cp.addLast(new ServerHandler(c.remoteAddress().toString.toIPList.toIPString, byteConsumer))
            }
          })
        val cf: ChannelFuture = bs.bind(port).sync()
        cf.channel().closeFuture().sync()
      } catch {
        case e : Throwable => e.printStackTrace()
      } finally {
        LOG.info("cf escape")
        parentGroup.shutdownGracefully()
        childGroup.shutdownGracefully()
      }
    }
  }
}

class myClientHandler(ip : String, byteConsumer: ByteConsumer) extends ChannelInboundHandlerAdapter {
  override def channelReadComplete(channelHandlerContext: ChannelHandlerContext): Unit = {

  }
  override def channelRead(channelHandlerContext: ChannelHandlerContext, o: scala.Any): Unit = {
    val vectorRecord : Vector[Record] = o.asInstanceOf[Vector[Record]]
    byteConsumer.read(ip, vectorRecord)
  }
  override def exceptionCaught(channelHandlerContext: ChannelHandlerContext, throwable: Throwable): Unit = {
    throwable.printStackTrace()
    channelHandlerContext.close()
  }
}

/* I need many slaveSock... (maybe..?) */
class SlaveClientSock(val ipList : List[String]) extends SlaveSock {
  val LOG = LogFactory.getLog(classOf[SlaveClientSock].getName)
  var ip2Sock : Map[String, Channel] = Map.empty
  val group = new NioEventLoopGroup()
  val byteConsumer = new ByteConsumer ()
  def start() =
  {
    if(!ip2Sock.isEmpty){
      try {
        val b: Bootstrap = new Bootstrap()
        b.group(group)
          .channel(classOf[NioSocketChannel])
          .handler(new ChannelInitializer[SocketChannel] {
            override def initChannel(c: SocketChannel): Unit = {
              val cp: ChannelPipeline = c.pipeline()
              cp.addLast(new Buf2VectorRecordDecode())
              cp.addLast(new myClientHandler(c.remoteAddress().toString.toIPList.toIPString, byteConsumer))
            }
          })

        ip2Sock = (for (ip <- clientList) yield {
          val cf: ChannelFuture = b.connect(ip._1, ip._2).sync()
          (ip._1, cf.channel())
        }).toMap
        for (cf <- ip2Sock.toList.map {
          _._2
        }) {
          cf.closeFuture().sync()
        }
      } catch {
        case e : Throwable => e.printStackTrace()
      } finally {
        LOG.info("cf escape")
      }

    }
  }
}





