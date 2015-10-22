package slave

object Main {
  
  def main(args: Array[String]) = {

    val client = new Slave("127.0.0.1:5959", "/data")

  }
}
