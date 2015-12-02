package master
import java.nio.channels._

object Main {
  
  def main(args: Array[String]) = {


    val slaveNum = try { 
      args(0).toInt
    } catch {
      case e : Exception => throw new Exception ("Argv[1] Error")
    }
    val server = new Master (slaveNum)
    server.start()

  }
}
