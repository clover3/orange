package master

//import common.typedef._

import scala.util.Sorting


object Main {
  
  def main(args: Array[String]) = {
    val server = Master;
//    var myArray : Array[String] = new Array[String](3)
//    myArray  = Array( "qat$}h(0]-" ,"maU5d)o?#t", "--R9@v&d-5")
//    myArray.foreach(x=>println(x))
//    Sorting.quickSort(myArray)
//    myArray.foreach(x=>println(x))
    // just example!
    //server.start(args(0).toInt)
    server.slaveThread.foreach(_.join())
    server.ipAddrList foreach println
    server.sorting_Key ()
    server.SendPartitions()
  }
}
