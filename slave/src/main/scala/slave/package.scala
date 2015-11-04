import java.io._
import java.net._
import java.nio._
import java.nio.channels._

import common.typedef._

import scala.concurrent.Future
import scala.io._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

package object slave {

  /* Call tree of SlaveCalculation
  getPartitions
   -> getSamples
     -> getNumFiles ( -> numFiles : Int )
     -> getSample( filePath -> Sample )
        -> parseLine ( line:String -> key:String)
   -> exchangeSample( List[Sample] -> Partitions )
      -> toBuffer( List[Sample] -> Buffer )
      -> exchangeBuffer( Buffer -> Buffer )  : Uses socket
  */

  trait SlaveSampler {
    def getPartition: Partitions
    def getSamples: Sample
    def parseLine(line: String): String
    def exchangeSample(samples: Sample): Partitions
    def exchangeSample(samplesBuffer: ByteBuffer) : ByteBuffer
  }

  object SlaveSampler {
    def apply(slaveSock: =>SlaveSocket, inputDirs_arg:List[String], outputDir_arg:String) = new SlaveSampler {
    def slaveSocket: SlaveSocket = slaveSock
    val inputDirs: List[String] = inputDirs_arg
    val ouputDir: String = outputDir_arg

    // number of keys for slave to send to server this number of keys' size sum up to 1MB
    //val totalSampleKey: Int = 100 * 1024
    val totalSampleKey: Int = totalSampleKeyPerSlave
    val linePerFile = 327680

    // getPartition
    // Role : reads local files and connects to server and receives partition
    // Calls 'getSamples' and 'exchangeSamples'
    def getPartition: Partitions = {
      exchangeSample(getSamples)
    }

    // getSamples
    // Role : 1. decides how many keys to extract from each file
    //        2.
    // If number of key is not divided by number of files
    // modulus remaining keys are taken from first file
    def getSamples: Sample = {

      def getFileList(dirPath: String): List[File] = {
        val d = new File(dirPath)
        if (d.exists && d.isDirectory)
          d.listFiles.filter(_.isFile).toList
        else
          throw new FileNotFoundException
      }

      val keyPerFile = totalSampleKey / getNumFiles
      val numListPre = for (i <- List.range(0, getNumFiles)) yield keyPerFile
      val numList = numListPre.head + (totalSampleKey % getNumFiles) :: numListPre.tail

      val fileList = inputDirs.flatMap(getFileList)
      assert((numList.size == fileList.size))

      val sampleList = fileList.zip(numList).map(pair => getSample(pair._1, pair._2))
      sampleList.toSample
    }

    // counts the number of files in the input directories
    def getNumFiles: Int = {
      def getNumFilesInDir(dir: String): Int = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).length
        }
        else
          0
      }
      val n = inputDirs.foldRight(0)((dir, sum) => sum + getNumFilesInDir(dir))
      if (n == 0)
        throw new FileNotFoundException
      else
        n
    }

    // get a sample from speicified file path
    def getSample(file: File, numSamples: Int): Sample = {
      // to avoid reading whole file
      //val numLines = Source.fromFile(file).getLines().size
      val numLines = linePerFile
      val fileStream: Stream[String] = Source.fromFile(file).getLines().toStream;
      val keyList = fileStream.take(numSamples).map(parseLine).toList

      (numLines, keyList)

    }

    // parseLine gets line containing both key and value, and return only key string
    //line.slice(0,10)   : Key
    //line.slice(13,44)  : Index
    //line.slice(46,98)  : Value
    def parseLine(line: String): String = {
      line.slice(0,10)
    }

    def exchangeSample(samples: Sample): Partitions = {
      parsePartitionBuffer(exchangeSample(samples.toBuffer))
    }

    // recieves buffer containing samples and returns buffer containing partitions
    def exchangeSample(samplesBuffer: ByteBuffer): ByteBuffer = {
      slaveSocket.sendAndRecvOnce(samplesBuffer)
    }
  }
}

  class Slave (val master : String, val inputDirs : List[String], val outputDir : String)  {
    //val slaveSocket = new SlaveSocket(master)
    var inputDir: List[String] = Nil

    def run() = {
      //val slaveCalculation = SlaveSampler(slaveSocket, inputDirs, outputDir)
      //val partitions : Partitions = slaveCalculation.getPartition
      //print("this is partition : "); println (partitions)
      //val ipList = partitions map {_._1}
      val ipList = List("192.168.10.1", "192.168.10.2")
      val slaveSock = SlaveSock(ipList)
      val f : Future[BigOutputFile] = slaveSock.recvData("127.0.0.1")
      println(f.isCompleted)
      println(ipList)
      println(InetAddress.getLocalHost.getHostAddress)
      ipList.map {ip => 
      if(InetAddress.getLocalHost.getHostAddress != ip) 
          slaveSock.sendData(ip, Future{???}, 1, 2)}
      Thread.sleep(6000)
      slaveSock.wakeup()
      ()

    }
  }

  class SlaveSocket(val master : String)  {
    val (masterIPAddr, masterPort) : (String, Int) = {
      val ipR = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):([0-9]+)""".r
      master match {
        case ipR(ip1,ip2,ip3,ip4,port) => (List(ip1, ip2, ip3, ip4).mkString("."), port.toInt)
        case _ => throw new Exception("IP error")
      }
    }
    val sock = SocketChannel.open(new InetSocketAddress(masterIPAddr, masterPort))

    def sendAndRecvOnce(buffer : ByteBuffer) : ByteBuffer = {
      var nbyte = 0
      nbyte = sock.write(buffer)
      buffer.clear()
      sock.read(buffer)
      buffer
    }
  }
}

