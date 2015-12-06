import java.net._
import java.nio.ByteBuffer
import java.util


import common.typedef._
import common.future._
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{Unpooled, ByteBuf}
import io.netty.channel._
import io.netty.channel.nio.{NioEventLoopGroup, NioEventLoop}
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.{ByteToMessageDecoder, ByteToMessageCodec}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.channel.socket._
import org.apache.commons.logging.{Log, LogFactory}

import scala.Array._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Sorting}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.annotation.tailrec

package object master {

  type slaveID = Int

  var Iplist : Map[String, slaveID] = Map.empty // save IPs from
  class Buf2Decode(val LOG : Log, val id : Int, val sockPromises : Map[slaveID, Promise[Unit]], val finishPromises : Map[slaveID, Promise[Unit]]) extends ByteToMessageDecoder {
  var sampleEnd = false
    override def decode(channelHandlerContext: ChannelHandlerContext, byteBuf: ByteBuf, list: util.List[AnyRef]): Unit = {
      val expectLen = totalSampleKeyPerSlave*10 + 8
      if (sampleEnd) {
        println(byteBuf.readableBytes())
        if(byteBuf.readableBytes() < 3) return
        else {
          byteBuf.markReaderIndex()
          var check = byteBuf.readByte()
          if(check == 79) {
            byteBuf.readBytes(2)
            sockPromises(id).complete(Success(()))
            LOG.info("recvSlaveRequest OK id : " + id)
            return
          }
          else if(check == 70) {
            byteBuf.readBytes(2)
            finishPromises(id).complete(Success(()))
            LOG.info("recvSlaveRequest FN id : " + id)
            return
          }
          else {
            var s = ""
            var check2 = false
            while (check != 0xa && byteBuf.readableBytes() > 0) {
              s = s + new String(Array(check))
              check = byteBuf.readByte()
              if (check == 0xa)
                check2 = true
            }
            if (check2 == true) {
              if(Iplist.contains(s)) {
                sockPromises(Iplist(s)).future onSuccess {
                  case u =>
                    val buffer = ByteBuffer.allocate(15)
                    buffer.put(s.getBytes())
                    buffer.flip()
                    val buf = Unpooled.wrappedBuffer(buffer)
                    channelHandlerContext.writeAndFlush(buf)
                    LOG.info("Send S " + s + " ->" + id)
                }
              }
              else {
                byteBuf.resetReaderIndex()
              }
            }
            else {
              byteBuf.resetReaderIndex()
            }
            return
          }
        }
      }
      else if(byteBuf.readableBytes() < expectLen) {
        return
      }
      else {
        println(byteBuf)
        val bytes : Array[Byte]= new Array(byteBuf.readableBytes())
        byteBuf.readBytes(bytes)
        val buf = ByteBuffer.wrap(bytes)
        list.add(buf)
        byteBuf.resetWriterIndex()
        println(byteBuf)
        sampleEnd = true
      }
    }
  }
  class ServerHandler(val num : Int, val id2buf : Map[slaveID, Promise[ByteBuffer]]) extends ChannelInboundHandlerAdapter {
    override def channelRead(channelHandlerContext: ChannelHandlerContext, msg:Object) = {
      val vB = msg.asInstanceOf[ByteBuffer]
      id2buf(num).complete(Success(vB))

    }
    override def channelReadComplete(channelHandlerContext: ChannelHandlerContext): Unit = {

    }

    override def exceptionCaught(channelHandlerContext: ChannelHandlerContext, cause: Throwable): Unit = {
      cause.printStackTrace()
      channelHandlerContext.close()
    }
  }

  class Master (val slaveNum : Int) {
    val LOG = LogFactory.getLog(classOf[Master].getName)
    val id2Slave : Map[slaveID, Promise[SocketChannel]] = (for(i <- 0 until slaveNum) yield {
      (i, Promise[SocketChannel]())
    }).toMap
    val id2SlaveByteBuf : Map[slaveID, Promise[ByteBuffer]] = (for(i <- 0 until slaveNum) yield {
      (i, Promise[ByteBuffer]())
    }).toMap
    val port : Int = 5959
    def myIp : String = InetAddress.getLocalHost().getHostAddress()
    val sockPromises: Map[slaveID, Promise[Unit]] =
      (for(i <- 0 until slaveNum) yield { (i, Promise[Unit]())}).toMap
    val finishPromises: Map[slaveID, Promise[Unit]] =
      (for(i <- 0 until slaveNum) yield { (i, Promise[Unit]())}).toMap
    val parentGroup = new NioEventLoopGroup(1)
    val childGroup = new NioEventLoopGroup(slaveNum)
    var acceptN = 0
    def getacceptNum() = {
      this.synchronized{
        val res = acceptN
        acceptN = acceptN + 1
        res
      }
    }
    def start() {
      try {
        val bs : ServerBootstrap = new ServerBootstrap()
        bs.group(parentGroup, childGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(c: SocketChannel): Unit = {
            val acceptNum = getacceptNum()
            if(acceptNum == slaveNum)
              return
            val ip = c.remoteAddress().toString.toIPList.toIPString
            LOG.info(ip)
            id2Slave(acceptNum).complete(Success(c))
            Iplist = Iplist + (ip -> acceptNum)
            val cp : ChannelPipeline = c.pipeline()
            cp.addLast(new Buf2Decode(LOG, acceptNum, sockPromises, finishPromises))
            cp.addLast(new ServerHandler(acceptNum, id2SlaveByteBuf))
          }
        })
        val cf : ChannelFuture = bs.bind(port).sync()
        SendPartitions()
        val futureList = finishPromises.toList.map{_._2.future}
        Await.result(all(futureList), Duration.Inf)
        close()
      } catch {
        case e : Throwable => e.printStackTrace()
      } finally {
        LOG.info("Master Server end")
        close()
      }
    }



    // sorting key and make partiton ( Array[String] -> Partition -> Partitions)
    def sorting_Key () : Partitions = {
      val keyArray : Array[String] = id2SlaveByteBuf.toList.map {
        case (id, bufPromise) =>
          val buf = Await.result(bufPromise.future, Duration.Inf)
          println(buf)
          parseSampleBuffer(buf)._2
      }.flatten.toArray
      val ips = Iplist.toList.map{_._1}.toArray
      Sorting.quickSort(keyArray)
      val keyArrLen = keyArray.length
      val ipLen = ips.length
      assert(ipLen != 0)
      val numSlave = keyArrLen/ipLen   // assume that Datas are uniform
      val keyLimitMin = 0.toChar.toString * 10
      val keyLimitMax = 126.toChar.toString * 10
      val pSeq = for( i<- 1 to ipLen )
        yield  {
          if (ipLen == 1)
            new Partition(ips(0), keyLimitMin, keyLimitMax)
          else if( i == 1)
            new Partition(ips(0), keyLimitMin , keyArray( (i) * numSlave) )
          else if( i != (ipLen) )
            new Partition( ips(i - 1), keyArray((i-1) * numSlave), keyArray( (i) * numSlave ) )
          else
            new Partition(ips(ipLen - 1), keyArray((i-1) * numSlave), keyLimitMax)
        }
      println("sorting key done")
      pSeq.toList
    }


    // comment
    //send partitions for each slaves (Partitions -> buffer)
    def SendPartitions (): Unit = {
      val partitions = sorting_Key()
      id2Slave.toList.foreach {
        case (id, slavefuture) =>
          slavefuture.future onSuccess {
            case c =>
              println("partitions befor write :  "  + partitions)
              println(c)
            c.writeAndFlush(Unpooled.wrappedBuffer(partitions.toByteBuffer))
          }
      }
    }

    def close(): Unit ={
      parentGroup.shutdownGracefully()
      childGroup.shutdownGracefully()
    }
  }
}
