package master

object Main {
  
  def main(args: Array[String]) = {

    val server = Master
    server.start(args(0).toInt)
    server.slaveThread.foreach(_.join())
    //server.slaveThread
    server.ipAddrList foreach println
    //println("dddd")
    server.sorting_Key ()
    server.SendPartitions()
  }
}
