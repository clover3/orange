package common

package object typedef {

  ///////////// Common Definition - must be shared between master and slave /////////////////////
  class Partition
  type Partitions = List[Partition]
  // Sample = (Number of total records, sampled records)
  type Sample = (Int, List[String])
  type Samples = List[Sample]
}