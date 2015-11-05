package slave

/**
 * Created by Clover on 2015-10-31.
 */
class ResourceChecker {
//memory info
  val mb = 1024 * 1024
  val runtime = Runtime.getRuntime

  // returns the memory that this program is using
  def usedMemory:Int = (runtime.totalMemory - runtime.freeMemory()).toInt
 //??? can i use int ? or Long type.

  // returns the remaining memory that this program can use
  def remainingMemory:Int = runtime.freeMemory.toInt

  // returns the total memory that this program can use
  def totalMemory:Int = runtime.totalMemory().toInt

  // returns the Max Memory
  def maxMemory : Int = runtime.maxMemory().toInt
}

