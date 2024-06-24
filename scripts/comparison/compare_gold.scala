import scala.io.Source

val ref = Source.fromFile(args(0)).getLines.map(_.toInt).toSet
val test = Source.fromFile(args(1)).getLines.map(_.toInt).toSet
val report  = Map.empty.withDefaultValue(0.0) ++ Source.fromFile(args(2)).
  getLines.drop(1).map(x => {
      val xs = x.split("\t")
      (xs(4).trim.toInt, xs(0).trim.toDouble)
    })

val tp = ref.intersect(test).size
val fp = (test -- ref).size
val fn = (ref -- test).size
val recall = tp.toDouble / ref.size
val precision = tp.toDouble / test.size

println(s"TP $tp FP $fp FN $fn")
println("Recall %.2f".format(recall) + " precision %.2f".format(precision))

val tpWeight = ref.intersect(test).toSeq.map(t => report(t)).sum
val fpWeight = (test -- ref).toSeq.map(t => report(t)).sum
val totWeight = tpWeight + fpWeight
println(s"Weighted precision %.2f".format(tpWeight/totWeight))
println(s"Weighted recall %.2f".format(totWeight))
