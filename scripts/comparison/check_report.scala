import scala.io.Source.stdin

for { line <- stdin.getLines.drop(1) } {
  val num = line.split("\t")(1).toLong
  if (num >= args(0).toLong) println(line)
}
