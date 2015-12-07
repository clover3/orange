package slave
import scala.Console._

object ProgressLogger{
  var array :List[(String,String,Int)] = Nil
  def updateLog(key : String, log :String, color : Int)  = {
    array = array.map{
      t => if( t._1 == key) (key, log, color)
      else t
    }
    if( !array.exists(t => t._1 == key) )
      array = array :+ (key,log, color)
    printLog()
  }
  def printLog() = {
    print("\033[2J")
    println(Console.BLUE + " <  Progress  >")
    array.foreach(t =>
      if(t._3 == 0)
        println(Console.RED + t._2)
      else
        println(Console.CYAN + t._2)
    )
    println(Console.RESET)
  }
}
