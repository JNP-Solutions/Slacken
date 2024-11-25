import scala.io.Source

/* Script to filter assembly_summary.txt. For use from download_genomic_library.sh.
* This script was tested with Scala 2.12 and may not work with Scala 2.13 or Scala 3.
*/

case class Record(taxid: Int, asmLevel: String) {
  //This definition of "complete" comes from Kraken2's rsync_from_ncbi.pl
  def complete: Boolean = asmLevel == "Complete Genome" ||
    asmLevel == "Chromosome"
}

val mode = args(0)
val file = args(1)

def lines = Source.fromFile(file).getLines().filter(! _.startsWith("#"))

def toRecord(l: String) = {
  val fs = l.split("\t")
  Record(fs(5).toInt, fs(11))
}

def records = lines.map(toRecord)

//taxa that have some complete genome (or chromosomes)
val completeTaxa = scala.collection.mutable.BitSet.empty ++ records.filter(_.complete).map(_.taxid)

for { l <- lines } {
  mode match {
    case "all" =>
      println(l)
    case "complete" =>
      if (toRecord(l).complete)
        println(l)
    case "prefer_complete" =>
      val rec = toRecord(l)
      if (rec.complete || !completeTaxa.contains(rec.taxid))
        println(l)
  }
}