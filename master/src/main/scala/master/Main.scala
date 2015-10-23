package master

object Main {
  
  def main(args: Array[String]) = {
    val server = Master;
    // just example!
    server.start(args(0).toInt)
    server.slaveThread.foreach(_.join())
    server.ipAddrList foreach println
  }
}
