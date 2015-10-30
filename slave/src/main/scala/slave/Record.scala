/**
 * Created by Clover on 2015-10-30.
 */

package slave

package object Record {

  type Record = (String, String)

  implicit class PartitionCompanionOps(val rec: Record) extends AnyVal {
    def key: String = rec._1
    def data: String = rec._2
  }
}