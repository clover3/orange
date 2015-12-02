package slave

/**
 * Created by Clover on 2015-10-31.
 */
class ResourceChecker {
//memory info
  val mb = 1024 * 1024
  val runtime = Runtime.getRuntime

  // returns the memory that this program is using
  def usedMemory:Long = (runtime.totalMemory - runtime.freeMemory())
 //??? can i use int ? or Long type.

  // returns the remaining memory that this program can use
  def remainingMemory:Long = runtime.freeMemory

  // returns the total memory that this program can use
  def totalMemory:Long = runtime.totalMemory()

  // returns the Max Memory
  def maxMemory : Long = runtime.maxMemory()
}

