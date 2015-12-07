package slave

import java.io.File
import java.net.{NetworkInterface, InetAddress}
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

import slave.Record._
import slave.socket.PartitionSocket

import common.typedef._
import scala.collection.JavaConversions._
import common.future._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Try}
import scala.async.Async.{async, await}


// given IP and bytes, write downs data into IBigFile
class ByteConsumer(val tempDir : String) {
  var outBigfileList: List[(Int,BigOutputFile)] = Nil
  var FileTotalLen : Int = 0
  var headFileSize : Int = 0
  var startedNewFile : Boolean = false
  var totalFileNum : Int = 0
  val init : Promise[Unit] = Promise[Unit]()
  val p : Promise[List[(BigOutputFile)]] = Promise[List[(BigOutputFile)]]()

  def read(sockIp: String, records: Vector[BRecord], end : Boolean): Unit = {

    if (outBigfileList.nonEmpty && startedNewFile)
      records foreach {record => outBigfileList.head._2.appendRecord(record)}
    else {
      val outBigfile = new BigOutputFile(tempDir + "/" + "runiel_is_cute_>_<_" + sockIp +"_" + totalFileNum)
      records foreach {record => outBigfile.appendRecord(record)}
      outBigfileList = (headFileSize,outBigfile)::outBigfileList
      startedNewFile = true
    }
    if(end) {
      outBigfileList.head._2.close()
      startedNewFile = false
      val one = FileTotalLen
      val percent = ((totalFileNum / one) * 100).toInt
      totalFileNum += 1
      val msg = sockIp + " - Recieved ||" + "=" * percent + ">" + " " * (100 - percent) + "||"
      ProgressLogger.updateLog(sockIp, msg)
    }

    if( init.isCompleted ){
      if(totalFileNum == FileTotalLen )
      {
        //println("I'm finished  (ip, totalFileNum, FileTotalLen) : " + sockIp)
        val msg = sockIp + " - [Receive Done]"
        ProgressLogger.updateLog(sockIp, msg)
        p.complete( Success(outBigfileList.map(_._2)) )
      }
    }

//    println("read record size ### " +records.size)
  }
  def resetSize(s : Int) : Unit = {headFileSize = s}
  def setFileTotalLen(s: Int) : Unit = {
    FileTotalLen = s
    init.complete(Success())
  }
  def get2BigfileList(): List[(Int,BigOutputFile)] = outBigfileList
  def getFileTotalLen(): Int = FileTotalLen
}


trait ShuffleSocket {
  val ipList: List[String]
  val socket: PartitionSocket
  lazy val slaveNum: Int = ipList.length
  var basePort: Int = 5000
  val masterPromise : Promise[Unit]
  val endPromise : Promise[Unit] = Promise[Unit]()
  lazy val ipPortList: List[(String, Int)] = ipList.map { ip: String => {
    basePort = basePort + 1; (ip, basePort)
  }
  }
  def getIPv4LocalAddress() : List[List[Int]] = {
    val b = NetworkInterface.getNetworkInterfaces()
    val listInterface = b.toList
    val listAddr = listInterface.map(i=>i.getInterfaceAddresses().toList).flatten
    val listIPv4Addr = listAddr.map(a=> a.getAddress().getAddress()).filter(addr => addr.size==4)
    def toIntArray(bArray:Array[Byte]):List[Int] = { bArray.map(b => b.toInt & 0xFF).toList }
    listIPv4Addr.map(toIntArray)
  }
  lazy val myIp: String = {
    val ipL = (getIPv4LocalAddress().map(_.toIPString)).filter{case ip => ipList.exists(_ == ip)}
    if(ipL.size == 1)
      ipL.head
    else
      throw new NoSuchElementException("ipLsize isn't 1")
  }
  lazy val (serverList, cList): (List[(String, Int)], List[(String, Int)]) = ipPortList.span(_._1 != myIp)
  lazy val myIpPort: (String, Int) = cList.head
  lazy val clientList = cList.tail
  val buf: ByteBuffer = ByteBuffer.allocate(100 * 10000) // I don't know how many I can use buffer...
  lazy val consumerPromises : Map[String, Promise[ByteConsumer]] = {
    val res : Map[String, Promise[ByteConsumer]]= (for( ip <- serverList ) yield { (ip._1 ,Promise[ByteConsumer]()) }).toMap
    val res2 : Map[String, Promise[ByteConsumer]] = (for (ip <- clientList) yield { (ip._1, Promise[ByteConsumer]()) }).toMap
    res ++ res2
  }
  val tempDir : String
  def dataFuture(ip:String): Future[List[BigOutputFile]] = {
    println("I will connect "+ip)
    val csFuture = consumerPromises(ip).future
    val p = Promise[List[BigOutputFile]]()
    csFuture.onSuccess {
      case cs => {
        p.completeWith(cs.p.future)
      }
    }
    p.future
  }
}


trait newShuffleSock {
  def recvData(ip : String) : List[BigOutputFile]
  def sendData(ip: String, file: IBigFile, st: Int, ed: Int): Unit
  def sendSize(ip: String, size: Int) : Unit
  val promise : Promise[Unit]
  def death() : Unit
  def write(cur_sock: Channel, file: IBigFile, st: Int, ed: Int): ChannelFuture = {
    cur_sock.writeAndFlush(Unpooled.wrappedBuffer(file.getRecords(st, ed).toMyBuffer))
  }
  def writeSize(cur_sock : Channel, size:Int) : ChannelFuture = {

    val bytearray = ByteBuffer.allocate(4).putInt(size).array
    val buf = Unpooled.wrappedBuffer(bytearray)

    cur_sock.write(buf)
  }
}

object newShuffleSock {
  def apply(partitions: Partitions, tempDir : String, socket : PartitionSocket) = new newShuffleSock {
    var d = new File(tempDir)
    if (!d.exists)
      d.mkdir()
    val ipList = partitions.map { _._1 }
    val p = Promise[Unit]()
    val slaveServerSock = new SlaveServerSock(ipList, tempDir, socket, p)
    val serverThread = new Thread(slaveServerSock)
    val slaveClientSock = new SlaveClientSock(ipList, tempDir, socket, p)
    val clientThread = new Thread(slaveClientSock)
    serverThread.start()
    clientThread.start()
    val promise = Promise[Unit]()
    def ip2Sock: Map[String, Future[Channel]] = slaveServerSock.ip2Sock ++ slaveClientSock.ip2Sock

    def recvData(ip: String): List[BigOutputFile] = {
      val f = {
        if(slaveServerSock.handles(ip))
        {
          slaveServerSock.dataFuture(ip)
        }
        else
        {
          slaveClientSock.dataFuture(ip)
        }
      }
      Await.result(f,Duration.Inf)
    }
    def getSock(ip: String): Future[Channel] = ip2Sock(ip)
    def sendData(ip: String, file: IBigFile, st: Int, ed: Int): Unit = {
      val sock = Await.result(getSock(ip),Duration.Inf)
      write(sock, file, st, ed).sync()
    }
    def sendSize(ip : String, size : Int) = {
      val sock = Await.result(getSock(ip), Duration.Inf)
      writeSize(sock, size)
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
      byteConsumer.setFileTotalLen(size)
      return
    }
    if (byteBuf.readableBytes() < 4) return
    if(!check2) {
      check2 = true
      val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
      byteConsumer.resetSize(size)
      if(size == 0)
      {
        val result : Vector[BRecord] = Vector()
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
 val result: Vector[BRecord] = (for (i <- Range(0, recordnum) if i + real_size < byteConsumer.headFileSize)
   yield {
     (byteBuf.readBytes(10).array(), (byteBuf.readBytes(90).array()))
   }).toVector
 if(real_size+ result.size == byteConsumer.headFileSize) {
   //println("real_size + result.size == byteConsumer.headFileSize : " + real_size , result.size, byteConsumer.headFileSize)
   check2 = false
   end = true
 }
 if(result.nonEmpty)
   list.add((result,!check2))
  }
}

class Buf2VectorRecordClientDecode(c : SocketChannel, byteConsumer : ByteConsumer, consumerPromises : Map[String, Promise[ByteConsumer]]) extends ByteToMessageDecoder {
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
      if(!consumerPromises(ip).isCompleted)
        consumerPromises(ip).complete(Success(byteConsumer))
      byteConsumer.setFileTotalLen(size)
      return
    }
    if (byteBuf.readableBytes() < 4) return
    if(!check2) {
      check2 = true
      val size = ByteBuffer.wrap(byteBuf.readBytes(4).array).getInt
      //println("resetSize : " + size + " ip : " + ip)
      byteConsumer.resetSize(size)
      if(size == 0)
      {
        val result : Vector[BRecord] = Vector()
        check2 = false
        end = true
        list.add((result, !check2))
      }
      return
    }
    if (byteBuf.readableBytes() < 100) return
    val recordnum: Int = byteBuf.readableBytes() / 100
  val real_size = cur_size
  val result: Vector[BRecord] = (for (i <- Range(0, recordnum) if i + real_size < byteConsumer.headFileSize)
    yield {
      (byteBuf.readBytes(10).array(), byteBuf.readBytes(90).array())
    }).toVector
  if(real_size + result.size == byteConsumer.headFileSize) {
  //  println("real_size + result.size == byteConsumer.headFileSize : " + real_size + result.size, byteConsumer.headFileSize)
    check2 = false
    end = true
  }
  if(result.nonEmpty)
    list.add((result,!check2))
  }
}

class ServerHandler(ip: String, byteconsumer: ByteConsumer) extends ChannelInboundHandlerAdapter {
  override def channelRead(channelHandlerContext: ChannelHandlerContext, msg: Object) = {
    val (vectorRecord , end): (Vector[BRecord], Boolean) = msg.asInstanceOf[(Vector[BRecord], Boolean)]
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
class SlaveServerSock(val ipList: List[String], val tempDir : String, val socket : PartitionSocket, val masterPromise : Promise[Unit]) extends ShuffleSocket with Runnable {
  val LOG = LogFactory.getLog(classOf[SlaveServerSock].getName)
  val sockPromises: Map[String, Promise[Channel]] =
    (for( ip <- serverList ) yield { (ip._1 ,Promise[Channel]()) }).toMap
  val ip2Sock: Map[String, Future[Channel]] =
    (for( ip <- serverList ) yield { (ip._1 ,sockPromises(ip._1).future) }).toMap
  val parentGroup = new NioEventLoopGroup(1)
  val childGroup = new NioEventLoopGroup()
  def handles(ip:String):Boolean = serverList.map(_._1).contains(ip)
  


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
              val ip : String = c.remoteAddress().toString.toIPList.toIPString
              sockPromises(ip).complete(Success(c))
              LOG.info(ip2Sock)
              val cp: ChannelPipeline = c.pipeline
              val byteConsumer = new ByteConsumer(tempDir)
              cp.addLast(new Buf2VectorRecordDecode(ip,byteConsumer))
              cp.addLast(new ServerHandler(ip, byteConsumer))
              consumerPromises(ip).complete(Success(byteConsumer))
            }
          })
        val cf: ChannelFuture = bs.bind(port).sync()
        val cfchannel = cf.channel()
        socket.sendok(masterPromise)
        cfchannel.closeFuture().sync()
      } catch {
        case e: Throwable => e.printStackTrace()
      } finally {
        LOG.info("cf escape")
        parentGroup.shutdownGracefully()
        childGroup.shutdownGracefully()
      }
    }
    else {
      socket.sendok(masterPromise)
    }
  }
}

class myClientHandler(c: SocketChannel, byteConsumer: ByteConsumer) extends ChannelInboundHandlerAdapter {
  override def channelReadComplete(channelHandlerContext: ChannelHandlerContext): Unit = {

  }

  override def channelRead(channelHandlerContext: ChannelHandlerContext, o: scala.Any): Unit = {
    val (vectorRecord , end): (Vector[BRecord], Boolean) = o.asInstanceOf[(Vector[BRecord], Boolean)]
    byteConsumer.read(c.remoteAddress().toString.toIPList.toIPString, vectorRecord, end)
  }

  override def exceptionCaught(channelHandlerContext: ChannelHandlerContext, throwable: Throwable): Unit = {
    throwable.printStackTrace()
    channelHandlerContext.close()
  }
}

/* I need many slaveSock... (maybe..?) */
class SlaveClientSock(val ipList: List[String], val tempDir : String, val socket : PartitionSocket, val masterPromise : Promise[Unit]) extends ShuffleSocket with Runnable {
  val LOG = LogFactory.getLog(classOf[SlaveClientSock].getName)
  val sockPromises: Map[String, Promise[Channel]] =
    (for( ip <- clientList ) yield { (ip._1 ,Promise[Channel]()) }).toMap
  val ip2Sock: Map[String, Future[Channel]] =
    (for( ip <- clientList ) yield { (ip._1 ,sockPromises(ip._1).future) }).toMap
  val group = new NioEventLoopGroup()
  var sockCount = 0
  var socket2ByteConsumer : Map[SocketChannel,ByteConsumer] = Map.empty

  def death() = group.shutdownGracefully()

  def run() = {
    if (clientList.nonEmpty) {
      try {
        val b: Bootstrap = new Bootstrap()
        b.group(group)
          .channel(classOf[NioSocketChannel])
          .handler(new ChannelInitializer[SocketChannel] {
            override def initChannel(c: SocketChannel): Unit = {
              val byteConsumer = new ByteConsumer(tempDir)
              val cp: ChannelPipeline = c.pipeline()
              cp.addLast(new Buf2VectorRecordClientDecode(c, byteConsumer, consumerPromises))
              cp.addLast(new myClientHandler(c, byteConsumer))
            }
          })

        Await.result(masterPromise.future, Duration.Inf)

        val cflist = for (ip <- clientList) yield {
            val p : Promise[String] = Promise[String]()
            socket.checkMasterRequest(ip._1, p)
            val ipString = Await.result(p.future, Duration.Inf)
            val cf: ChannelFuture = b.connect(ipString, ip._2).sync()
            val c = cf.channel()
            sockPromises(ipString).complete(Success(c))
            c
        }


        //val clist = Await.result(all(cflist), Duration.Inf)
        
        socket.sendfinish(masterPromise.future, endPromise)

        for (cf <- cflist ) {
          cf.closeFuture().sync()
        }
      } catch {
        case e: Throwable => e.printStackTrace()
      } finally {
        LOG.info("cf escape")
        Await.result(endPromise.future, Duration.Inf)
        group.shutdownGracefully()
      }
    }
    else {
      print("FN start")
      socket.sendfinish(masterPromise.future, endPromise)
      Await.result(endPromise.future, Duration.Inf)
    }
  }
}




