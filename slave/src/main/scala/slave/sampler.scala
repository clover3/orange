package slave

import java.io.{FileNotFoundException, File}
import java.nio.ByteBuffer

import common.typedef._
import slave.socket.PartitionSocket

import scala.io.Source


package object Sampler {

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

    def exchangeSample(samplesBuffer: ByteBuffer): ByteBuffer
  }

  object SlaveSampler {
    def apply(slaveSock: => PartitionSocket, inputDirs_arg: List[String], outputDir_arg: String) = new SlaveSampler {
      def slaveSocket: PartitionSocket = slaveSock

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
        line.slice(0, 10)
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

}