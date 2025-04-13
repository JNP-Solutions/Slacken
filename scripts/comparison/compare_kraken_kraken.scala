import scala.io.Source


//kraken report format file (reference)
def refFile = Source.fromFile(args(0)).getLines.drop(1)
val refVector = kvector(refFile)
val totalReads = refFile.next.split("\t")(1).toLong

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
def compareSample(file: String): Unit = {
  def lines = Source.fromFile(file).getLines.drop(1)
  val sampleVector = kvector(lines)
  val allKeys = sampleVector.keySet ++ refVector.keySet

  val diffSquares = allKeys.iterator.map(x => Math.pow((sampleVector(x) - refVector(x)), 2))
  val lse = Math.sqrt(diffSquares.sum)

  val l1 = allKeys.iterator.map(x => Math.abs(sampleVector(x) - refVector(x))).sum

  println(s"$file\t$lse\t$l1")
}

compareSample(args(1))
