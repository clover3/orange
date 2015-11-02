package slave

/**
 * Created by Clover on 2015-10-31.
 */
trait IResourceChecker {
  val mb = 1024 * 1024

  // returns the memory that this program is using
  def usedMemory:Int

  // returns the remaining memory that this program can use
  def remainingMemory:Int = totalMemory - usedMemory

  // returns the total memory that this program can use
  def totalMemory:Int
}

// delete abstract after implementing methods
abstract class ResourceChecker extends IResourceChecker
{

}
