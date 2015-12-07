package slave
import scala.Console._

object ProgressLogger{
  var array :List[(String,String, String,Int)] = Nil
  def updateLog(key : String, head :String, log :String, color : Int)  = {
    array = array.map{
      t => if( t._1 == key) (key, head, log, color)
      else t
    }
    if( !array.exists(t => t._1 == key) )
      array = array :+ (key,head, log, color)
    printLog()
  }
  def printLog() = {
    print("\033[2J")
    val maxHeadLen = {
      array.foldLeft(0) {
        case (max, (_, s, _, _)) => if(max < s.length) s.length else max
      }
    }
    println(Console.BLUE + " <  Progress  >")
    array.foreach(t => {
      print(Console.GREEN + t._2 + " "*(maxHeadLen - t._2.length + 2))
      if (t._4 == 1) {
        println(Console.RED + t._3)
      }
      else
        println(Console.CYAN + t._3)
    }
    )
    println(Console.RESET)
  }
}
