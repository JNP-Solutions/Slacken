import scala.io.Source

/*
 Compares two bracken-style reports using the LSE method.
 */

val file1 = Source.fromFile(args(0)).getLines.drop(1)
val file2 = Source.fromFile(args(1)).getLines.drop(1)

def vector(lines: Iterator[String]) = lines.map(x => x.split("\t")).
  map(xs => ((xs(1).toInt, xs(6).toDouble))).toMap.withDefaultValue(0.0)

val v1 = vector(file1)
val v2 = vector(file2)

val allKeys = v1.keySet ++ v2.keySet

//for { k <- allKeys } {
//  println(v1(k) + " " + v2(k))
//}

val diffSquares = allKeys.iterator.map(x => Math.pow((v1(x) - v2(x)), 2))
val lse = Math.sqrt(diffSquares.sum)
println(lse)
