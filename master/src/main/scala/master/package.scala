import java.net._
import java.nio._
import java.nio.channels._

import common.typedef._

import scala.Array._
import scala.util.Sorting
import scala.util.control.Breaks._

package object master {
/*  class IA(val ipAddress : String) {
 *     def toIntList: List[Int] = ???
 *
 * }
 */




  implicit class StringCompanionOps(val s: String) extends AnyVal {
    def toIPList : List[Int] = {
      val R = "/(.*):[0-9]+".r
      val R2 = """(\d{1,3})\.(\d{1,3})\.( \d{1,3})\.(\d{1,3})""".r
      s match {
        case R(ip) => {ip.split('.').map(_.toInt).toList}
        case R2(ip1,ip2,ip3,ip4) => List(ip1.toInt, ip2.toInt, ip3.toInt, ip4.toInt)
        case _ => {throw new Exception("IP error")}
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
    var KeyList : List[String] = Nil
    var IpList : List[String] = Nil
    var ClientsocketList : List[SocketChannel] =Nil  // to write buffer
    var KeyArray : Array[String] = empty // save sample datas from each slaves
    var IpArray : Array[String] = ipAddrList.toArray // save IPs from
    var partition : Partition = new Partition("","","")
    var partitions : Partitions =Nil
    val port : Int = 5959


    var Samples : List[String] = Nil

    def myIp : String = InetAddress.getLocalHost().getHostAddress()

    def start(slaveNum : Int) {
      val server = ServerSocketChannel.open()
      val sock  = server.socket()


      //val pool : ExecutorService = Executors.newFixedThreadPool(slaveNum) //ThreadPool



      sock.bind(new InetSocketAddress(port))


      var acceptNum = 0
      println("Listening...")
      breakable {
        while (true) {
          if(acceptNum >= slaveNum) {sock.close(); break}
          val client = server.accept()
          ClientsocketList = client :: ClientsocketList

          acceptNum = acceptNum + 1
          println("Connected")
          addIPList(client.socket().getRemoteSocketAddress().toString())
          val slave = new Slave(acceptNum, client, client.socket().getRemoteSocketAddress().toString().toIPList.toIPString, KeyList, IpList)
          //pool.execute(slave)
          id2Slave = id2Slave + (acceptNum -> slave)
          val t = new Thread(slave)
          addSlaveThread(t)
          t.start()

        }
      }
    }


    def addIPList(ipaddr : String) {
      object IPOrdering extends Ordering[List[Int]] {
        def compare(a : List[Int], b:List[Int]) = a.head compare b.head
      }
      ipAddrList = (ipaddr::ipAddrList).map(_.toIPList).sorted(IPOrdering).map(_.toIPString)
    }

    def addSlaveThread(t : Thread) {
      slaveThread = t :: slaveThread
    }

    // sorting key and make partiton ( Array[String] -> Partition -> Partitions)
    def sorting_Key (){
      KeyArray = KeyList.toArray
      val d = KeyArray
      val ips = IpArray
      Sorting.quickSort(d)

      val x = d.length
      val y = ips.length
      val z = x/y   // assume that Datas are uniform

      var a :Int = 0
      for (a<- Range(0, y-1)){
        if (a == 0) {
           partitions = new Partition(ips(0), 0.toChar.toString*10 , d(a*z + z-1) ) ::partitions/
        }
        else if(a==(y-1)){
          var add : Partition = new Partition(ips(y-1), d(a*z), 127.toChar.toString*10)
            partitions = add :: partitions //aski
        }
        else{
           partitions = new Partition( ips(a), d(a*z), d(a*z + z-1) )::partitions
        }
      }

    }

    //send partitions for each slaves (Partitions -> buffer)
    def SendPartitions (): Unit ={
      ClientsocketList.foreach(x=>x.write(partitions.toByteBuffer))
    }
  }
  

  class Slave (val id : slaveID, val sock : SocketChannel, val ip : String, val KeyList : List[String], val IpList : List[String]) extends Runnable {
    /* *********STructure ********
    readSampleData(buffer -> Key : Array[String], Ip : Array[String]) //read key and ip
    ->SortingAndMakePartition(d:Array[String],ips :Array[String]) :Sorting keys and Make Partition to each Ip
         Sort & (Array[String] ->Partition-> Partitions)
    ->Write(Partitions->buffer) (((In server ,not each thread)))

     */

    //ParseBuffer and Convert to String and Save to Array{string] (Buffer -> samples)_
    def ParseBuffer(buffer: ByteBuffer) = {
      val sample : Sample = parseSampleBuffer(buffer)
      KeyList :: sample._2  //??? Is it right expression?? I wnat to add each Sample KeyList to All KeyList

    }

    //read key and ip  & save those to Array{string]
    def readSampleData(buffer : ByteBuffer) ={
      buffer.clear()
      sock.read(buffer)
      ParseBuffer(buffer)

    }



    def givePartition(buffer : ByteBuffer) = {
      buffer.clear()
      sock.read(buffer)
      //Sampledatas += buffer.toString
// input buffer handler(consider partition range...?)
// and consider write buffer content..
// below case jest test...
        println("hihihihhii")
        println(buffer.get(0))
        buffer.clear()
//        sock.write(buffer)
        sock.write(ByteBuffer.wrap("hi!".getBytes()))


////////////////////////////////////////////////////

/*        if(buffer.hasRemaining()) {
        buffer.compact()
        } else {
        buffer.clear
        }
  */
    }

//    def write(partitions : Partitions): Unit ={
//      val buf:ByteBuffer = partitions.toByteBuffer;
//      sock.write(buf)
//    }

                                                                                                                                                                                                                                                                                                               
    def run()
    {
      
      println("Hi!")
// just example!  I don't know buffer capacity uuu..
      val Buffer = ByteBuffer.allocate(1024 * 1024* 1024 * 1024 * 1024)
      readSampleData(Buffer)


      //givePartition(inOutBuffer)
      //sock.close()
    }
  }


}
