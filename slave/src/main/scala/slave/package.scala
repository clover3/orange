import java.net._
import java.io._
import java.nio.Buffer
import scala.io._
import common.typedef._

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
      -> toPartition( Buffer -> Partitions )
  */

  trait SlaveCalculation {
    val master: String
    val inputDirs : List[String]
    val ouputDir : String

    // number of keys for slave to send to server this number of keys' size sum up to 1MB
    val totalSampleKey : Int = 100 * 1024


    // getPartition
    // Role : reads local files and connects to server and receives partition
    // Calls 'getSamples' and 'exchangeSamples'
    def getPartition : Partitions


    // getSamples
    // Role : 1. decides how many keys to extract from each file
    //        2.
    def getSamples : List[Sample] = {
      val keyPerFile = totalSampleKey / getNumFiles
      def getFileList(dirPath : String) : List[File] ={
        val d = new File(dirPath)
        if( d.exists && d.isDirectory )
          d.listFiles.filter(_.isFile).toList
        else
          throw new FileNotFoundException
      }
      val fileList = inputDirs.flatMap(getFileList)
      var numListPre = for( i <- List.range(0, getNumFiles) ) yield keyPerFile
      var numList = numListPre.head + (totalSampleKey % getNumFiles) :: numListPre.tail
      assert((numList.size == fileList.size))
      fileList.zip(numList)
      List()
    }

    // counts the number of files in the input directories
    def getNumFiles : Int = {
      def getNumFilesInDir(dir: String) : Int = {
        val d = new File(dir)
        if( d.exists && d.isDirectory )
          d.listFiles.filter(_.isFile).length
        else
          0
      }
      val n = inputDirs.foldRight(0)( (dir, sum) => sum + getNumFilesInDir(dir) )
      if( n == 0 )
        throw new FileNotFoundException
      else
        n
    }


    // get a sample from speicified file path
    def getSample(filePath : String, numSamples : Int) : Sample =
    {
      val file = new File(filePath)
      val numLines = Source.fromFile(file).getLines().size
      val fstream: Stream[String] = Source.fromFile(file).getLines().toStream;
      val keyList = fstream.take(numSamples).map(parseLine).toList
      (numLines, keyList)
    }
    // parseLine gets line containing both key and value, and return only key string
    def parseLine(line : String) : String = {
      line.split(' ')(0)
    }

    def exchangeSample(samples : List[Sample]) : Partitions

    // write down List[Samples] to buffer
    def toBuffer(samples: List[Sample]) : Buffer

    // recieves buffer containing samples and returns buffer containing partitions
    def exchangeSample(samplesBuffer: Buffer) : Buffer

    // parse buffer to make Partitions
    def toPartition(samplesBuffer: Buffer) : Partitions
  }


  class Slave (val master : String, val outputDir : String) {
    val (masterIPAddr, masterPort) : (String, Int) = {
      val ipR = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):([0-9]+)""".r
      master match {
        case ipR(ip1,ip2,ip3,ip4,port) => (List(ip1, ip2, ip3, ip4).mkString("."), port.toInt)
        case _ => throw new Exception("IP error")
      }
    }
    val sock = new Socket(masterIPAddr, masterPort)
    var inputDir : List[String] = Nil
    def addinputDir(inDir : String) = {
        inputDir = inDir::inputDir
    }
    def start = {

    }
  }
}
