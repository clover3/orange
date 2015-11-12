package slave

import java.net.InetAddress
import java.nio.ByteBuffer
import io.netty.bootstrap.{ServerBootstrap, Bootstrap}
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioSocketChannel, NioServerSocketChannel}
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.commons.logging.LogFactory
import slave.BigOutputFile
import slave.Record._
import common.typedef._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Try}
import scala.async.Async.{async, await}

// given IP and bytes, write downs data into IBigFile
class ByteConsumer {
  var outBigfileList: List[(Int,BigOutputFile)] = Nil
  var FileTotalLen : Int = 0
  var headFileSize : Int = 0
  var startedNewFile : Boolean = false
  var totalFileNum : Int = 0
  val init : Promise[Unit] = Promise[Unit]()
  val p : Promise[List[(BigOutputFile)]] = Promise[List[(BigOutputFile)]]()

  def read(sockIp: String, records: Vector[Record], end : Boolean): Unit = {

    if (outBigfileList.nonEmpty && startedNewFile)
      records foreach {record => outBigfileList.head._2.appendRecord(record)}
    else {
      val outBigfile = new BigOutputFile("runiel_is_cute_>_<_" + sockIp +"_" + totalFileNum)
      records foreach {record => outBigfile.appendRecord(record)}
      outBigfileList = (headFileSize,outBigfile)::outBigfileList
      startedNewFile = true
    }
    if(end) {
      outBigfileList.head._2.close()
      startedNewFile = false
      totalFileNum += 1
    }

    if( init.isCompleted && totalFileNum == FileTotalLen )
      p.complete( Success(outBigfileList.map(_._2)) )

//    println("read record size ### " +records.size)
  }
  def resetSize(s : Int) : Unit = {headFileSize = s}
  def setFileTotalLen(s: Int) : Unit = {
    FileTotalLen = s
    init.complete(Success(0))
  }
  def get2BigfileList(): List[(Int,BigOutputFile)] = outBigfileList
  def getFileTotalLen(): Int = FileTotalLen
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
  lazy val (serverList, cList): (List[(String, Int)], List[(String, Int)]) = ipPortList.span(_._1 != myIp)
  lazy val myIpPort: (String, Int) = cList.head
  lazy val clientList = cList.tail

  val buf: ByteBuffer = ByteBuffer.allocate(100 * 10000) // I don't know how many I can use buffer...

  val consumerPromises :Map[String, Promise[ByteConsumer]] =
    (for( ip <- serverList ) yield { ip._1 -> (Promise[ByteConsumer]()) }).toMap

  def dataFuture(ip:String): Future[List[BigOutputFile]] = {
    val csFuture = consumerPromises(ip).future
    val p = Promise[List[BigOutputFile]]()
    csFuture.onComplete {
      case Success(cs) => p.completeWith(cs.p.future)
    }
    p.future
  }
}


trait newShuffleSock {
  def recvData(ip : String) : Future[List[BigOutputFile]]
  def sendData(ip: String, file: IBigFile, st: Int, ed: Int): Unit
  def sendSize(ip: String, size: Int) : Unit
  val promise : Promise[Unit]
  def death() : Unit
  def write(cur_sock: Channel, file: IBigFile, st: Int, ed: Int): ChannelFuture = {
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

  def apply(partitions: Partitions) = new newShuffleSock {
    val ipList = partitions.map { _._1 }
    val slaveServerSock = new SlaveServerSock(ipList)
    val serverThread = new Thread(slaveServerSock)
    val slaveClientSock = new SlaveClientSock(ipList)
    val clientThread = new Thread(slaveClientSock)
    serverThread.start()
    clientThread.start()
    val promise = Promise[Unit]()


    def ip2Sock: Map[String, Channel] = slaveServerSock.ip2Sock ++ slaveClientSock.ip2Sock


    def recvData(ip: String): Future[List[BigOutputFile]] = {
      if(slaveServerSock.handles(ip))
        slaveServerSock.dataFuture(ip)
      else
        slaveClientSock.dataFuture(ip)
    }


    def getSock(ip: String): Future[Channel] = Future {
      var check: Channel = null
      while (check == null) {
        Thread.sleep(100)
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
      write(sock, file, st, ed).sync()
    }

    def sendSize(ip : String, size : Int) = {
      val sock = Await.result(getSock(ip), Duration.Inf)
      writeSize(sock, size).sync()
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
    //println("check2 " + check2 +" in ip : " + ip + " byte : " + byteBuf.readableBytes())
    def cur_size : Int = {
      if(end){
        end = false
        0
      }
      else if(byteConsumer.outBigfileList.nonEmpty)
        byteConsumer.outBigfileList.head._2.size
      else
        0
    }

    if (byteBuf.readableBytes() < 4) return
    if (!check) {
      check = true
      val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
      println("getSize : " + size + " ip : " + ip)
      byteConsumer.setFileTotalLen(size)
      return
    }
    if (byteBuf.readableBytes() < 4) return
    if(!check2) {
      check2 = true
      val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
      println("resetSize : " + size + " ip : " + ip)
      byteConsumer.resetSize(size)
      if(size == 0)
      {
        val result : Vector[Record] = Vector()
        check2 = false
        end = true
        list.add((result, !check2))
      }
      return
    }
    if (byteBuf.readableBytes() < 100) return
    val recordnum: Int = byteBuf.readableBytes() / 100
    val real_size = cur_size
    /*println("readableBytes : " + byteBuf.readableBytes)
    println("recordnum : " + recordnum)
    println("real_size : " + real_size)
    println("byteConsumer.size" + byteConsumer.headFileSize) */
    val result: Vector[Record] = (for (i <- Range(0, recordnum) if i + real_size < byteConsumer.headFileSize)
      yield {
        (new String(byteBuf.readBytes(10).array()), new String(byteBuf.readBytes(90).array()))
      }).toVector
    if(real_size+ result.size == byteConsumer.headFileSize) {
      println("real_size + result.size == byteConsumer.headFileSize : " + real_size , result.size, byteConsumer.headFileSize)
      check2 = false
      end = true
    }
    //println(result)
    if(result.nonEmpty)
      list.add((result,!check2))
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
      else if(byteConsumer.outBigfileList.nonEmpty)
        byteConsumer.outBigfileList.head._2.size
      else
        0
    }
    if (byteBuf.readableBytes() < 4) return
    if (!check) {
      check = true
      val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
      println("getSize : " + size + " ip : " + ip)
      byteConsumer.setFileTotalLen(size)
      return
    }
    if (byteBuf.readableBytes() < 4) return
    if(!check2) {
      check2 = true
      val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
      //println(byteBuf)
      println("resetSize : " + size + " ip : " + ip)
      byteConsumer.resetSize(size)
      if(size == 0)
      {
        val result : Vector[Record] = Vector()
        check2 = false
        end = true
        list.add((result, !check2))
      }
      return
    }
    if (byteBuf.readableBytes() < 100) return
    val recordnum: Int = byteBuf.readableBytes() / 100
    val real_size = cur_size
    val result: Vector[Record] = (for (i <- Range(0, recordnum) if i + real_size < byteConsumer.headFileSize)
      yield {
        (new String(byteBuf.readBytes(10).array()), new String(byteBuf.readBytes(90).array()))
      }).toVector
    if(real_size + result.size == byteConsumer.headFileSize) {
      println("real_size + result.size == byteConsumer.headFileSize : " + real_size + result.size, byteConsumer.headFileSize)
      check2 = false
      end = true
    }
    //println(result)
    if(result.nonEmpty)
      list.add((result,!check2))
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
  def handles(ip:String):Boolean = serverList.contains(ip)


  def death() = {
    parentGroup.shutdownGracefully()
    childGroup.shutdownGracefully()
  }

  def run(): Unit = {
    if (serverList.nonEmpty) {
      try {
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
              val byteConsumer = new ByteConsumer
              cp.addLast(new Buf2VectorRecordDecode(ip,byteConsumer))
              cp.addLast(new ServerHandler(ip, byteConsumer))
              consumerPromises(ip).complete(Success(byteConsumer))
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
  var socket2ByteConsumer : Map[SocketChannel,ByteConsumer] = Map.empty

  def sockToIP(socket: SocketChannel) : String = {
    val tryIp = Try(socket.remoteAddress.toString.toIPList.toIPString).toOption
    tryIp match {
      case Some(ip) => (ip)
    }
  }

  def death() = group.shutdownGracefully()

  def run() = {
    println("clientList : " + clientList)
    if (clientList.nonEmpty) {
      try {
        Thread.sleep(1000)
        val b: Bootstrap = new Bootstrap()
        b.group(group)
          .channel(classOf[NioSocketChannel])
          .handler(new ChannelInitializer[SocketChannel] {
            override def initChannel(c: SocketChannel): Unit = {
              val byteConsumer = new ByteConsumer
              val cp: ChannelPipeline = c.pipeline()
              cp.addLast(new Buf2VectorRecordClientDecode(c, byteConsumer))
              cp.addLast(new myClientHandler(c, byteConsumer))
              consumerPromises(sockToIP(c)).complete(Success(byteConsumer))
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




