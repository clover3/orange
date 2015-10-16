import java.net._
import java.io._
import scala.io._

package object slave {
  

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
