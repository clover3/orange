import java.net._
import java.nio._
import java.nio.channels._

import common.typedef._

import scala.Array._
import scala.util.Sorting
import scala.util.control.Breaks._

package object master {
  
  var KeyList : List[String] = Nil
  var partition : Partition = new Partition("","","")
  var partitions : Partitions =Nil





  implicit class StringCompanionOps(val s: String) extends AnyVal {
    def toIPList : List[Int] = {
      val R = "/(.*):[0-9]+".r
      val R2 = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})""".r
      s match {
        case R(ip) => {ip.split('.').map(_.toInt).toList}
        case R2(ip1,ip2,ip3,ip4) => List(ip1.toInt, ip2.toInt, ip3.toInt, ip4.toInt)
        case _ => {throw new Exception("IP error:"+s)}
      }
    }
  }
  
  implicit class ListCompanionOps(val l: List[Int]) extends AnyVal {
    def toIPString : String = {l.map{_.toString}.mkString(".")}
  }




  type slaveID = Int



  object Master {
    var ipAddrList : List[String] = Nil
    var slaveThread : List[Thread] = Nil
    var id2Slave : Map[slaveID, Slave] = Map.empty
    var ClientsocketList : List[SocketChannel] =Nil  // to write buffer
    var KeyArray : Array[String] = empty // save sample datas from each slaves
    var IpArray : Array[String] = ipAddrList.toArray // save IPs from
    partition  = new Partition("","","")
    partitions =Nil
    val port : Int = 5959
    val server = ServerSocketChannel.open()
    val sock  = server.socket()


    var Samples : List[String] = Nil

    def myIp : String = InetAddress.getLocalHost().getHostAddress()

    def start(slaveNum : Int) {
      KeyList =Nil

      sock.bind(new InetSocketAddress(port))

      var acceptNum = 0
      println("Listening...")
      breakable {
        while (true) {
          if(acceptNum >= slaveNum) break
          val client = server.accept()
          ClientsocketList = client :: ClientsocketList

          acceptNum = acceptNum + 1
          val addrStr = client.socket().getRemoteSocketAddress().toString()
          addIPList(addrStr)
          println(addrStr.toIPList.toIPString)
          val slave = new Slave(acceptNum, client, addrStr.toIPList.toIPString)
          id2Slave = id2Slave + (acceptNum -> slave)
          val t = new Thread(slave)
          addSlaveThread(t)
          t.start()
        }
      }
      slaveThread.foreach(_.join())
      sorting_Key()
      SendPartitions()
      close()
    }


    def addIPList(ipaddr : String) {
      ipAddrList = (ipaddr::ipAddrList).map(_.toIPList).map(_.toIPString)
    }

    def addSlaveThread(t : Thread) {
      slaveThread = t :: slaveThread
    }

    // sorting key and make partiton ( Array[String] -> Partition -> Partitions)
    def sorting_Key (){
      var keyArray = KeyList.toArray
      val ips = ipAddrList.toArray
      Sorting.quickSort(keyArray)
      val keyArrLen = keyArray.length
      val ipLen = ips.length
      assert(ipLen != 0)
      val numSlave = keyArrLen/ipLen   // assume that Datas are uniform
      val keyLimitMin = 0.toChar.toString * 10
      val keyLimitMax = 126.toChar.toString * 10
      partitions = Nil
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

      partitions = pSeq.toList
      println("sorting key done")
    }

    // comment
    //send partitions for each slaves (Partitions -> buffer)
    def SendPartitions (): Unit ={
      println("partitions befor write :  "  + partitions)
      ClientsocketList.foreach(x=>x.write(partitions.toByteBuffer))
    }

    def close(): Unit ={
      ClientsocketList.foreach(x=>x.close())
      server.close()
    }
  }
  

  class Slave (val id : slaveID, val sock : SocketChannel, val ip : String) extends Runnable {
    /* *********STructure ********
    readSampleData(buffer -> Key : Array[String], Ip : Array[String]) //read key and ip
    ->SortingAndMakePartition(d:Array[String],ips :Array[String]) :Sorting keys and Make Partition to each Ip
         Sort & (Array[String] ->Partition-> Partitions)
    ->Write(Partitions->buffer) (((In server ,not each thread)))

     */

    //ParseBuffer and Convert to String and Save to Array{string] (Buffer -> samples)_
    def ParseBuffer(buffer: ByteBuffer) = {

      val sample : Sample = parseSampleBuffer(buffer)
      val numSampleKey :Int = sample._2.length
      var KeyListForRead = sample._2
      val TestArray = KeyListForRead.toArray
      var Keyindex : Int  = 0
      for (Keyindex <- Range(0,numSampleKey)) {
        KeyList = KeyListForRead.head :: KeyList //??? Is it right expression?? I wnat to add each Sample KeyList to All KeyList
        KeyListForRead = KeyListForRead.tail
      }

    }

    //read key and ip  & save those to Array{string]
    def readSampleData(buffer : ByteBuffer) ={
      buffer.clear()
      var nbyte = 0
      var i = 0
      val expectLen = totalSampleKeyPerSlave*10 + 8
      while(i <  expectLen) {
      nbyte = sock.read(buffer)
      i = i + nbyte
      }
      ParseBuffer(buffer)

    }

                                                                                                                                                                                                                                                                                                              
    def run()
    {

      val Buffer = ByteBuffer.allocate(1024 * 1024 * 2)

      readSampleData(Buffer)
      Buffer.clear()

    }
  }


}
