import java.net._
import java.nio._
import java.nio.channels._

import common.typedef._
import common.future._

import scala.Array._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Sorting}
import scala.concurrent.ExecutionContext.Implicits.global

package object master {
  



  type slaveID = Int


  class Master (val slaveNum : Int) {
    var id2Slave : Map[slaveID, SlaveManager] = Map.empty
    def Iplist : List[(slaveID, String)] = id2Slave.toList.map{case (id, slave) => (id, slave.ip)} // save IPs from
    val port : Int = 5959
    val server = ServerSocketChannel.open()
    val sock  = server.socket()
    def myIp : String = InetAddress.getLocalHost().getHostAddress()
    val sockPromises: Map[slaveID, Promise[Unit]] =
      (for(i <- 0 until slaveNum) yield { (i, Promise[Unit]())}).toMap
    val finishPromises: Map[slaveID, Promise[Unit]] =
      (for(i <- 0 until slaveNum) yield { (i, Promise[Unit]())}).toMap

    def start() {

      sock.bind(new InetSocketAddress(port))

      println("Listening...")
      val slaveThread : List[Thread] = {
        for (acceptNum <- 0 until slaveNum) 
        yield {
          val client = server.accept()

          val addrStr = client.socket().getRemoteSocketAddress().toString()
          println(addrStr.toIPList.toIPString)
          val slave = new SlaveManager(acceptNum, client, addrStr.toIPList.toIPString)
          id2Slave = id2Slave + (acceptNum -> slave)
          val t = new Thread(slave)
          t.start()
          t
        }
      }.toList
      slaveThread.foreach(_.join())
      SendPartitions()
      recvRequest()
      close()
    }



    // sorting key and make partiton ( Array[String] -> Partition -> Partitions)
    def sorting_Key () : Partitions = {
      val keyArray : Array[String] = id2Slave.toList.map{case (id, slave) => slave.ParseBuffer()}.flatten.toArray 
      val ips = id2Slave.toList.map{case (id, slave) => slave.ip}.toArray
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

    def recvSlaveRequest (id : slaveID, sSock : SocketChannel) : Unit = {
        val buffer = ByteBuffer.allocate(15)
        sSock.read(buffer)
        buffer.clear()
        val s = new String(buffer.array(), "ASCII")
        if (s == "OK") {
          sockPromises(id).complete(Success(()))
        }
        else if (s == "FN") {
          finishPromises(id).complete(Success(()))
        }
        else {
        val sip = (Iplist.find(_._2 == s))
        for (sopt <- sip) yield {
          if (sockPromises(sopt._1).isCompleted) {
            buffer.put("S".getBytes())
            sSock.write(buffer)
          }
          else {
            buffer.put("U".getBytes())
            sSock.write(buffer)
          }
          throw new Exception("strange string is coming from" + sSock)
        }
      }
    }

    def recvRequest() : Unit = {
      val fList = id2Slave.toList.map {
        case(id, slave) => Future {
          def loopfunction(id: slaveID, sock: SocketChannel): Unit = {
            if (finishPromises(id).isCompleted) {
            }
            else {
              recvSlaveRequest(id, sock)
              loopfunction(id, sock)
            }
          }
          loopfunction(id, slave.sock)
        }
      }
      Await.result(all(fList), Duration.Inf)
    }

    // comment
    //send partitions for each slaves (Partitions -> buffer)
    def SendPartitions (): Unit = {
      val partitions = sorting_Key()
      println("partitions befor write :  "  + partitions)
      id2Slave.toList.map{case (id, slave) => slave.sock}.foreach(x=>x.write(partitions.toByteBuffer))
    }

    def close(): Unit ={
      id2Slave.toList.map{case (id, slave) => slave.sock}.foreach(x=>x.close())
      server.close()
    }
  }
  

  class SlaveManager (val id : slaveID, val sock : SocketChannel, val ip : String) extends Runnable {
    /* *********STructure ********
    readSampleData(buffer -> Key : Array[String], Ip : Array[String]) //read key and ip
    ->SortingAndMakePartition(d:Array[String],ips :Array[String]) :Sorting keys and Make Partition to each Ip
         Sort & (Array[String] ->Partition-> Partitions)
    ->Write(Partitions->buffer) (((In server ,not each thread)))

     */

    val Buffer = ByteBuffer.allocate(1024 * 1024 * 2)
    //ParseBuffer and Convert to String and Save to Array{string] (Buffer -> samples)_
    def ParseBuffer() : List[String] = {

      val sample : Sample = parseSampleBuffer(Buffer)
      val KeyListForRead : List[String] = sample._2
      KeyListForRead
    }

    //read key and ip  & save those to Array{string]
    def readSampleData(buffer : ByteBuffer) : Unit ={
      buffer.clear()
      val expectLen = totalSampleKeyPerSlave*10 + 8
      var i = 0
      var nbyte = 0
      while(i <  expectLen) {
      nbyte = sock.read(buffer)
      i = i + nbyte
      }
    }

                                                                                                                                                                                                                                                                                                              
    def run()
    {
      readSampleData(Buffer)
    }
  }


}
