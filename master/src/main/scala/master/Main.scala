package master



//import common.typedef._


object Main {
  
  def main(args: Array[String]) = {
    val server = Master
    server.start(args(0).toInt)
    server.slaveThread.foreach(_.join())
    server.ipAddrList foreach println
    server.sorting_Key ()
    server.SendPartitions()
  }
}
