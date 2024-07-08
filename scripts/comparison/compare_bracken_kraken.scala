import scala.io.Source

/** Compares a bracken-style file (the sample) with a kraken report-style file (the reference).
  Also computes taxon set TP, FP, FN, precision, recall.
  */

//bracken format file
val file1 = Source.fromFile(args(0)).getLines.drop(1)
//kraken report format file (reference)
val file2 = Source.fromFile(args(1)).getLines.drop(1)

def vector(lines: Iterator[String]) = lines.map(x => x.split("\t")).
  map(xs => ((xs(1).toInt, xs(6).toDouble))).toMap.withDefaultValue(0.0)

//NB keeps ONLY "S" level lines from the kreport
def kvector(lines: Iterator[String]) = lines.map(x => x.split("\t")).
  flatMap(xs => if (xs(3) == "S") Some(((xs(4).toInt, xs(0).toDouble/100)))
    else None).toMap.withDefaultValue(0.0)

val v1 = vector(file1)
val v2 = kvector(file2)

val allKeys = v1.keySet ++ v2.keySet

//for { k <- allKeys } {
//  println(v1(k) + " " + v2(k))
//}

val diffSquares = allKeys.iterator.map(x => Math.pow((v1(x) - v2(x)), 2))
val lse = Math.sqrt(diffSquares.sum)
println("%.3g".format(lse))

val ref = v2.keySet
val test = v1.keySet

val tp = ref.intersect(test).size
val fp = (test -- ref).size
val fn = (ref -- test).size
val recall = tp.toDouble / ref.size
val precision = tp.toDouble / test.size

println(s"TP $tp FP $fp FN $fn")
println("Recall %.2f".format(recall) + " precision %.2f".format(precision))
