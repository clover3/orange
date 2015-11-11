package slave

import java.io.File


package object util {

  implicit class FileMonads(f: File) {
    def check = if (f.exists) Some(f) else None //returns "Maybe" monad
    def remove = if (f.delete()) Some(f) else None //returns "Maybe" monad
  }

  def deleteIfExist(path :String) = {
    for {
      foundFile <- new File(path).check
      deletedFile <- foundFile.remove
    } yield deletedFile
  }

  def profile[R](code: => R, t: Long = System.currentTimeMillis()) = (code, System.currentTimeMillis() - t)
}

