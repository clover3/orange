package slave

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey._
import io.netty.bootstrap.{ServerBootstrap, Bootstrap}
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
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

package socket {

  // given IP and bytes, write downs data into IBigFile
  class ByteConsumer {
    var ip2inBigfile: Map[String, List[(Int,BigOutputFile)]] = Map.empty
    var ip2size : Map[String, Int] = Map.empty
    var inBigFile: List[(String, Promise[IBigFile])] = Nil
    var size : Int = 0
    var newFile : Boolean = false
    var fileNum : Int = 0
    def read(sockIp: String, records: Vector[Record], end : Boolean): Unit = {

      if (ip2inBigfile.contains(sockIp) && newFile)
        records map {record => ip2inBigfile(sockIp).head._2.appendRecord(record)}
      else {
        val outBigfile = new BigOutputFile("runiel_is_cute_>_<" + fileNum)
        records map {record => outBigfile.appendRecord(record)}
        if(ip2inBigfile.contains(sockIp))
          ip2inBigfile = ip2inBigfile.updated(sockIp,(size,outBigfile)::ip2inBigfile(sockIp))
        else
          ip2inBigfile = ip2inBigfile + (sockIp -> List((size, outBigfile)))
        newFile = true
      }
      if(end) {
        ip2inBigfile(sockIp).head._2.close()
        newFile = false
        fileNum += 1
      }

      println("read record size ### " +records.size)
    }
    def resetSize(s : Int) : Unit = {size = s}
    def setSize(ip : String, s: Int) : Unit = {
      ip2size = ip2size + (ip -> s)
    }
    def get2Map(): Map[String, List[(Int,BigOutputFile)]] = ip2inBigfile
    def get2Size():Map[String, Int] = ip2size
  }


  trait ShuffleSocket {
    val ipList: List[String]
    lazy val slaveNum: Int = ipList.length
    var basePort: Int = 5000
    lazy val ipPortList: List[(String, Int)] = ipList.map { ip: String => {
      basePort = basePort + 1; (ip, basePort)
    }
    }
    val myIp: String = InetAddress.getLocalHost.getHostAddress.toIPList.toIPString
    /*val myIp = "127.0.0.1"*/
    lazy val (serverList, cList): (List[(String, Int)], List[(String, Int)]) = ipPortList.span(_._1 != myIp)
    lazy val myIpPort: (String, Int) = cList.head
    lazy val clientList = cList.tail

    val buf: ByteBuffer = ByteBuffer.allocate(100 * 10000) // I don't know how many I can use buffer...
  }
    

  trait newShuffleSock {
    def recvData(ip : String) : Future[List[BigOutputFile]]
    def sendData(ip: String, file: IBigFile, st: Int, ed: Int): Unit
    def sendSize(ip: String, size: Int) : Unit
    def death() : Unit
    def write(cur_sock: Channel, file: IBigFile, st: Int, ed: Int): ChannelFuture = {
      println("write hi")

          cur_sock.writeAndFlush(Unpooled.wrappedBuffer(file.getRecords(st, ed).toMyBuffer))
 }
    def writeSize(cur_sock : Channel, size:Int) : ChannelFuture = {

      println("writesize : " + size)
      val buf = Unpooled.directBuffer(4)
      buf.writeInt(size)
      cur_sock.writeAndFlush(buf)
    }
    
}

  object ShuffleSocket {

    def apply(ips: List[String]) = new newShuffleSock {
      val ipList = ips
      val slaveServerSock = new SlaveServerSock(ipList)
      val serverThread = new Thread(slaveServerSock)
      val slaveClientSock = new SlaveClientSock(ipList)
      val clientThread = new Thread(slaveClientSock)
      serverThread.start()
      clientThread.start()

      def ip2Bigfile: Map[String, List[(Int,BigOutputFile)]] = slaveServerSock.byteConsumer.get2Map() ++ slaveClientSock.byteConsumer.get2Map()

      def ip2Sock: Map[String, Channel] = slaveServerSock.ip2Sock ++ slaveClientSock.ip2Sock

      def ip2Size = slaveServerSock.byteConsumer.get2Size() ++ slaveClientSock.byteConsumer.get2Size()

      def recvData(ip: String): Future[List[BigOutputFile]] = Future {
        var check: List[BigOutputFile] = Nil
        while (check.isEmpty) {
          ip2Bigfile.get(ip) match {
            case Some(l) => ip2Size.get(ip) match {
              case Some(size) =>
              {
          /*      println("list size" + l.size)
                println("size" + size)
                println("head file size" + l.head._2.size)
                println("head size" + l.head._1)*/
                if(l.size == size && l.head._2.size == l.head._1) 
                  {check = l.map(_._2)}
              }
              case None =>
            }
            case None =>
          }
        }
        println("entire file size" + check.size)
        check
      }

      def getSock(ip: String): Future[Channel] = Future {
        var check: Channel = null
        while (check == null) {
          //print("in getSock")
          ip2Sock.get(ip) match {
            case Some(sock) => check = sock
            case None =>
          }
        }
        check
      }

      def sendData(ip: String, file: IBigFile, st: Int, ed: Int): Unit = {
        val sock = Await.result(getSock(ip),Duration.Inf)
        println("sock in sendData")
        write(sock, file, st, ed).await()
      }

      def sendSize(ip : String, size : Int) = {
        val sock = Await.result(getSock(ip), Duration.Inf)
        println("sock in sendSize")
        writeSize(sock, size).await()
      }

      def death() = {
        slaveServerSock.death()
        slaveClientSock.death()
      }
    }
  }


  class Buf2VectorRecordDecode(ip : String, byteConsumer : ByteConsumer) extends ByteToMessageDecoder {
    var check = false
    var check2 = false
    var end = false
    override def decode(channelHandlerContext: ChannelHandlerContext, byteBuf: ByteBuf, list: java.util.List[AnyRef]): Unit = {
      def cur_size : Int = {
        println("end in cur_size   :   " + end) 
        if(end){
          end = false
          0
        }
        else if(byteConsumer.ip2inBigfile.contains(ip))
          byteConsumer.ip2inBigfile(ip).head._2.size
        else
          0
      }

      if (byteBuf.readableBytes() < 4) {
        return
      }
      else if (!check){
        check = true
        val size = ByteBuffer.wrap(byteBuf.readBytes(4).array()).getInt()
        println("getSize : " + size) 
        byteConsumer.setSize(ip, size)
        return
      }
      else if (byteBuf.readableBytes() < 4) {
        return
      }
      else if(!check2) {
        check2 = true
        val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
        println("resetSize : " + size)
        byteConsumer.resetSize(size)
        return
      }
      else if (byteBuf.readableBytes() < 100) {
        return;
      }
      else {
      val recordnum: Int = byteBuf.readableBytes() / 100
      
      val real_size = cur_size
      println("readableBytes : " + byteBuf.readableBytes)
      println("recordnum : " + recordnum)
      println("real_size : " + real_size)
      println("byteConsumer.size" + byteConsumer.size)
      val result: Vector[Record] = (for (i <- Range(0, recordnum) if(i + real_size < byteConsumer.size))
        yield {
          (new String(byteBuf.readBytes(10).array()), new String(byteBuf.readBytes(90).array()))
        }).toVector
      if(real_size+ result.size == byteConsumer.size) {
        check2 = false
        end = true
      }
      println(result)
      println("check2" + check2)
      if(!result.isEmpty)
        list.add((result,!check2))
      }
    }
  }

  class Buf2VectorRecordClientDecode(c : SocketChannel, byteConsumer : ByteConsumer) extends ByteToMessageDecoder {
    var check = false
    var check2 = false
    var end = false
    override def decode(channelHandlerContext: ChannelHandlerContext, byteBuf: ByteBuf, list: java.util.List[AnyRef]): Unit = {
    val ip = c.remoteAddress().toString.toIPList.toIPString
    def cur_size : Int = {
          if(end) {
          end = false
            0
          }
          else if(byteConsumer.ip2inBigfile.contains(ip))
            byteConsumer.ip2inBigfile(ip).head._2.size
          else
            0
        }
      if (byteBuf.readableBytes() < 4) {
        return
      }
      else if (!check){
        check = true
        val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
        println("getSize : " + size) 
        byteConsumer.setSize(ip, size)
        return
      }
      else if (byteBuf.readableBytes() < 4) {
        return
      }
      else if(!check2) {
        check2 = true
        val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
        println("resetSize : " + size)
        byteConsumer.resetSize(size)
        return
      }
      else if (byteBuf.readableBytes() < 100) {
        return;
      }
      else {
        val recordnum: Int = byteBuf.readableBytes() / 100
        val real_size = cur_size
        val result: Vector[Record] = (for (i <- Range(0, recordnum) if(i + real_size < byteConsumer.size))
          yield {
            (new String(byteBuf.readBytes(10).array()), new String(byteBuf.readBytes(90).array()))
          }).toVector
        if(real_size + result.size == byteConsumer.size) {
          check2 = false
          end = true
        }
        println(result)
        if(!result.isEmpty)
          list.add((result,!check2))
        return
      }
    }
  }

  class ServerHandler(ip: String, byteconsumer: ByteConsumer) extends ChannelInboundHandlerAdapter {
    override def channelRead(channelHandlerContext: ChannelHandlerContext, msg: Object) = {
      val (vectorRecord , end): (Vector[Record], Boolean) = msg.asInstanceOf[(Vector[Record], Boolean)]
      byteconsumer.read(ip, vectorRecord, end)
    }

    override def channelReadComplete(channelHandlerContext: ChannelHandlerContext): Unit = {

    }

    override def exceptionCaught(channelHandlerContext: ChannelHandlerContext, cause: Throwable): Unit = {
      cause.printStackTrace()
      channelHandlerContext.close()
    }
  }

  /* I need one serverSock.. (maybe..?) */
  class SlaveServerSock(val ipList: List[String]) extends ShuffleSocket with Runnable {
    val LOG = LogFactory.getLog(classOf[SlaveServerSock].getName)
    var ip2Sock: Map[String, Channel] = Map.empty
    val parentGroup = new NioEventLoopGroup(1)
    val childGroup = new NioEventLoopGroup()
    val byteConsumer = new ByteConsumer()

    def death() = {
      parentGroup.shutdownGracefully()
      childGroup.shutdownGracefully()
    }

    def run(): Unit = {
      if (!serverList.isEmpty) {
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
              val ip : String = c.remoteAddress().toString.toIPList.toIPString
              cp.addLast(new Buf2VectorRecordDecode(ip,byteConsumer))
              cp.addLast(new ServerHandler(ip, byteConsumer))
            }
          })
          val cf: ChannelFuture = bs.bind(port).sync()
          cf.channel().closeFuture().sync()
        } catch {
          case e: Throwable => e.printStackTrace()
        } finally {
          LOG.info("cf escape")
          parentGroup.shutdownGracefully()
          childGroup.shutdownGracefully()
        }
      }
    }
  }

  class myClientHandler(c: SocketChannel, byteConsumer: ByteConsumer) extends ChannelInboundHandlerAdapter {
    override def channelReadComplete(channelHandlerContext: ChannelHandlerContext): Unit = {

    }

    override def channelRead(channelHandlerContext: ChannelHandlerContext, o: scala.Any): Unit = {
      val (vectorRecord , end): (Vector[Record], Boolean) = o.asInstanceOf[(Vector[Record], Boolean)]
      byteConsumer.read(c.remoteAddress().toString.toIPList.toIPString, vectorRecord, end)
    }

    override def exceptionCaught(channelHandlerContext: ChannelHandlerContext, throwable: Throwable): Unit = {
      throwable.printStackTrace()
      channelHandlerContext.close()
    }
  }

  /* I need many slaveSock... (maybe..?) */
  class SlaveClientSock(val ipList: List[String]) extends ShuffleSocket with Runnable {
    val LOG = LogFactory.getLog(classOf[SlaveClientSock].getName)
    var ip2Sock: Map[String, Channel] = Map.empty
    val group = new NioEventLoopGroup()
    val byteConsumer = new ByteConsumer()

    def death() = group.shutdownGracefully()

    def run() = {
      print("clientList : "); println(clientList)
      if (!clientList.isEmpty) {
        try {
          Thread.sleep(1000)
          val b: Bootstrap = new Bootstrap()
          b.group(group)
            .channel(classOf[NioSocketChannel])
            .handler(new ChannelInitializer[SocketChannel] {
            override def initChannel(c: SocketChannel): Unit = {
              val cp: ChannelPipeline = c.pipeline()
              cp.addLast(new Buf2VectorRecordClientDecode(c, byteConsumer))
              cp.addLast(new myClientHandler(c, byteConsumer))
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
          case e: Throwable => e.printStackTrace()
        } finally {
          LOG.info("cf escape")
          group.shutdownGracefully()
        }

      }
    }
  }


}

