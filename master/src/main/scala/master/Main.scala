package master

object Main {
  
  def main(args: Array[String]) = {
    val server = new Master;
    server.start(2)
    server.slaveThread.foreach(_.join())
    server.ipAddrList foreach println
  }
}
