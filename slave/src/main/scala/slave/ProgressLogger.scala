package slave

object ProgressLogger{
  var array :List[(String,String)] = Nil
  def updateLog(key : String, log :String)  = {
    array = array.map{
      t => if( t._1 == key) (key, log)
      else t
    }
    if( !array.exists(t => t._1 == key) )
      array = array :+ (key,log)
    printLog()
  }
  def printLog() = {
    println(Console.CYAN + " <  Progress  >")
    array.foreach(t => println(t._2) )
    print(Console.RESET)
  }
}
