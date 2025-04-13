import scala.io.Source

/** Compares a bracken-style file (the sample) with a kraken report-style file (the reference).
  Also computes taxon set TP, FP, FN, precision, recall.
  */


//kraken report format file (reference)
def refFile = Source.fromFile(args(0)).getLines.drop(1)
val refVector = kvector(refFile)
val sampleFiles = args.drop(1)
val totalReads = refFile.next.split("\t")(1).toLong

def vector(lines: Iterator[String]) = lines.map(x => x.split("\t")).
  map(xs => ((xs(1).toInt, xs(6).toDouble))).toMap.withDefaultValue(0.0)

//NB keeps ONLY "S" level lines from the kreport
def kvector(lines: Iterator[String]) = lines.map(x => x.split("\t")).
  flatMap(xs => if (xs(3) == "S") Some(((xs(4).toInt, xs(0).toDouble/100)))
  else None).toMap.withDefaultValue(0.0)

val l10 = Math.log(10)
//Add a number smaller than expected in the inputs, to make comparison with 0 safe as log(0) is undefined
def safeLog(x: Double) =
  if (x == 0.0) Math.log(0.000001)/l10 else Math.log(x)/l10

def logRef(x: Int) = safeLog(refVector(x))

/** Compare a single sample (bracken format) against the reference */
def compareBrackenSample(file: String): Unit = {
  //This file is already assumed to be filtered at species level.

  def lines = Source.fromFile(file).getLines.drop(1)
  val sampleVector = vector(lines)
  val addedReads = lines.map(l => l.split("\t")(4).toLong).sum
  val addedFraction = addedReads.toDouble / totalReads
  def logSample(x: Int) = safeLog(sampleVector(x))

  val allKeys = sampleVector.keySet ++ refVector.keySet

  /*
for { k <- allKeys } {
  val tp = if (v2.keySet.contains(k)) "TP" else "FP"
  println(s"$k\t" + "%.2g".format(v1(k)) + "\t%.2g".format(v2(k)) + "\t%.2f".format(l1(k)) + "\t" + "%.2f".format(l2(k)) + s"\t$tp")
}
*/

  val diffSquares = allKeys.iterator.map(x => Math.pow((sampleVector(x) - refVector(x)), 2))
  val lse = Math.sqrt(diffSquares.sum)

  val diffSquaresLog = allKeys.iterator.map(x => Math.pow((logSample(x) - logRef(x)), 2))
  val lseLog = Math.sqrt(diffSquaresLog.sum)

  val l1 = allKeys.iterator.map(x => Math.abs(sampleVector(x) - refVector(x))).sum
  val l1Log = allKeys.iterator.map(x => Math.abs(logSample(x) - logRef(x))).sum

  val ref = refVector.keySet
  val test = sampleVector.keySet

  val tp = ref.intersect(test).size
  val fp = (test -- ref).size
  val fn = (ref -- test).size
  val recall = tp.toDouble / ref.size
  val precision = tp.toDouble / test.size

  //Extract some variables from expected filename patterns
  val pattern = """(.*)/(.+)_(\d+)_(\d+)_s(\d+)_c([\d.]+)_classified/S(\d+)_bracken""".r
  file match {
    case pattern(group, library, k, m, s, c, sample) =>
      println("%s\t%s\t%s\t%s\t%.3g\t%.3g\t%.3g\t%.3g\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f".
        format(group, library, c, sample, lse, lseLog, l1, l1Log, tp, fp, fn, precision, recall, addedFraction))
    case _ =>
      throw new Exception(s"Unexpected file name: $file")
  }

}

//headers
println(s"Group\tLibrary\tc\tSample\tLSE\tLSE(log10)\tL1\tL1(log10)\tTP\tFP\tFN\tPrecision\tRecall\tAdded reads")
for { file <- sampleFiles }
  compareBrackenSample(file)


