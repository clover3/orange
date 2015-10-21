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
    // Role : 1. decides how many keys to extract from each key
    //        2.
    def getSamples : List[Sample]

    // counts the number of files in the input directories
    def getNumFiles : Int

    // get a sample from speicified file path
    def getSample(filePath : String, numSamples : Int) : Sample
    // parseLine gets line containing both key and value, and return only key string
    def parseLine(line : String) : String

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
