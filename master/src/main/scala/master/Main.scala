package master
import java.nio.channels._

object Main {
  
  def main(args: Array[String]) = {

    val server = Master
    val slaveNum = try { 
      args(0).toInt
    } catch {
      case e : Exception => throw new Exception ("Argv[1] Error")
    }
    server.start(slaveNum)

  }
}
