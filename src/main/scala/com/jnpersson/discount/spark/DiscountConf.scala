/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.discount.spark

import com.jnpersson.discount.{Both, ForwardOnly, Frequency, Given}
import com.jnpersson.discount.bucket.Reducer
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.sql.SparkSession

/**
 * Command-line configuration for Discount. Run the tool with --help to see the various arguments.
 *
 * @param args command line arguments
 */
//noinspection TypeAnnotation
private[jnpersson] class DiscountConf(args: Array[String])(implicit spark: SparkSession)
  extends SparkConfiguration(args) {
  version(s"Discount ${getClass.getPackage.getImplementationVersion} beta (c) 2019-2022 Johan Nyström-Persson")
  banner("Usage:")
  shortSubcommandsHelp(true)

  def readIndex(location: String): Index =
    Index.read(location)

  val inputFiles = trailArg[List[String]](descr = "Input sequence files", required = false)
  val indexLocation = opt[String](name = "index", descr = "Input index location")
  val synthetic = opt[Long](descr = "Generate synthetic reads (specify # millions of reads, length 101 bp)",
    hidden = true)
  val min = opt[Int](descr = "Filter for minimum k-mer abundance", noshort = true)
  val max = opt[Int](descr = "Filter for maximum k-mer abundance", noshort = true)

  def discount(): Discount = {
    requireSuppliedK()
    new Discount(k(), parseMinimizerSource, minimizerWidth(), ordering(), sample(), maxSequenceLength(), normalize(),
      method(), partitions())
  }

  /** The index of input data, which may be either constructed on the fly from input sequence files,
   * or read from a pre-stored index created using the 'store' command. */
  def inputIndex(compatIndexLoc: Option[String] = None): Index = {
    requireOne(inputFiles, indexLocation, synthetic)
    if (indexLocation.isDefined) {
      readIndex(indexLocation())
    } else {
      val kmerReader =  compatIndexLoc match {
        case Some(ci) =>
          //Construct an index on the fly, but copy settings from a pre-existing index
          println(s"Copying index settings from $ci")
          val p = IndexParams.read(ci)
          Discount(p.k, Path(s"${ci}_minimizers.txt"), p.m, Given,
            sample(), maxSequenceLength(), normalize(), method(), partitions = partitions())
        case _ => discount //Default settings
      }
      synthetic.toOption match {
        case Some(n) => kmerReader.syntheticIndex(n * 1000000)
        case _ => kmerReader.index(inputFiles(): _*)
      }
    }
  }

  val count = new RunCmd("count") {
    banner("Count or export k-mers in input sequences or an index.")
    val output = opt[String](descr = "Location where the output is written", required = true)

    val tsv = opt[Boolean](default = Some(false),
      descr = "Use TSV output format instead of FASTA, which is the default")

    val superkmers = opt[Boolean](default = Some(false),
      descr = "Instead of k-mers and counts, output human-readable superkmers and minimizers")
    val histogram = opt[Boolean](default = Some(false),
      descr = "Output a histogram instead of a counts table")
    val buckets = opt[Boolean](default = Some(false),
      descr = "Instead of k-mer counts, output per-bucket summaries (for minimizer testing)")

    validate(inputFiles, superkmers) { (ifs, skm) =>
      if (skm && ifs.isEmpty) Left("Input sequence files required for superkmers.")
      else Right(Unit)
    }

    def run(): Unit = {
      lazy val index = inputIndex().filterCounts(min.toOption, max.toOption)
      val orientation = if (normalize()) ForwardOnly else Both
      def counts = index.counted(orientation)

      if (superkmers()) {
        discount.kmers(inputFiles() : _*).segments.writeSupermerStrings(output())
      } else if (buckets()) {
        index.writeBucketStats(output())
      } else if (histogram()) {
        index.writeHistogram(output())
      } else if (tsv()) {
        counts.writeTSV(output())
      } else {
        counts.writeFasta(output())
      }
    }
  }
  addSubcommand(count)

  val stats = new RunCmd("stats") {
    banner("Compute aggregate statistics for input sequences or an index.")
    val output = opt[String](descr = "Location where k-mer stats are written (optional)")

    requireOne(inputFiles, indexLocation, synthetic)

    def run(): Unit =
      Output.showStats(inputIndex().stats(min.toOption, max.toOption), output.toOption)
  }
  addSubcommand(stats)

  val store = new RunCmd("store") {
    banner("Store k-mers in a new optimized index.")
    val compatible = opt[String](descr = "Location of index to copy settings from, for compatibility")
    val output = opt[String](descr = "Location where the new index is written", required = true)

    def run(): Unit = {
      inputIndex(compatible.toOption).write(output())
      Index.read(output()).showStats()
    }
  }
  addSubcommand(store)

  val intersect = new RunCmd("intersect") {
    banner("Intersect sequence files or an index with other indexes.")
    val inputs = opt[List[String]](descr = "Locations of additional indexes to intersect with", required = true)
    val output = opt[String](descr = "Location where the intersected index is written", required = true)
    val rule = choice(Seq("max", "min", "left", "right", "sum"), default = Some("min"),
      descr = "Intersection rule for k-mer counts (default min)").map(Reducer.parseRule)

    def run(): Unit = {
      val index1 = inputIndex(inputs().headOption)
      val intIdxs = inputs().map(readIndex)
      index1.intersectMany(intIdxs, rule()).write(output())
      Index.read(output()).showStats()
    }
  }
  addSubcommand(intersect)

  val union = new RunCmd("union") {
    banner("Union sequence files or an index with other indexes.")
    val inputs = opt[List[String]](descr = "Locations of additional indexes to union with", required = true)
    val output = opt[String](descr = "Location where the result is written", required = true)
    val rule = choice(Seq("max", "min", "left", "right", "sum"), default = Some("sum"),
      descr = "Union rule for k-mer counts (default sum)").map(Reducer.parseRule)

    def run(): Unit = {
      val index1 = inputIndex(inputs().headOption)
      val unionIdxs = inputs().map(readIndex)
      index1.unionMany(unionIdxs, rule()).write(output())
      Index.read(output()).showStats()
    }
  }
  addSubcommand(union)

  val subtract = new RunCmd("subtract") {
    banner("Subtract indexes from another index or from sequence files.")
    val inputs = opt[List[String]](descr = "Locations of indexes B1, ... Bn in ((A - B1) - B2 ....)", required = true)
    val output = opt[String](descr = "Location where the result is written", required = true)
    val rule = choice(Seq("counters_subtract", "kmers_subtract"), default = Some("counters_subtract"),
      descr = "Difference rule for k-mer counts (default counters_subtract)").map(Reducer.parseRule)

    def run(): Unit = {
      val index1 = inputIndex(inputs().headOption)
      val subIdxs = inputs().map(readIndex)
      index1.subtractMany(subIdxs, rule()).write(output())
      Index.read(output()).showStats()
    }
  }
  addSubcommand(subtract)


  val presample = new RunCmd("sample") {
    banner("Sample m-mers to generate a minimizer ordering.")
    val output = opt[String](required = true, descr = "Location to write the sampled ordering at")

    validate(ordering, inputFiles) { (o, ifs) =>
      o match {
        case Frequency(_) =>
          if (ifs.isEmpty) Left("Input files required.") else Right(Unit)
        case _ => Left("Sampling requires the frequency ordering (-o frequency)")
      }
    }

    def run(): Unit =
      discount.kmers(inputFiles() :_*).constructSampledMinimizerOrdering(output())
  }
  addSubcommand(presample)

  val reindex = new RunCmd("reindex") {
    banner(
      """|Change the minimizer ordering of an index (may reduce compression). A specific ordering can be supplied
         |with -o given and --minimizers, or an existing index can serve as the template.
         |Alternatively, repartition an index into a different number of parquet buckets (or do both)""".stripMargin)

    val compatible = opt[String](descr = "Location of index to copy settings from, for compatibility")
    val output = opt[String](descr = "Location where the result is written", required = true)
    val pbuckets = opt[Int](descr = "Number of parquet buckets to repartition into")

    val changeMinimizers = toggle("changeMinimizers", descrYes = "Change the minimizer ordering (default: no)",
      default = Some(false))

    override def run(): Unit = {
      val compatParams = compatible.toOption.map(IndexParams.read)
      var in = inputIndex()

      if (changeMinimizers()) {
        val newSplitter: Broadcast[AnyMinSplitter] = compatParams match {
          case Some(cp) => cp.bcSplit
          case _ => spark.sparkContext.broadcast(discount.getSplitter(None))
        }
        in = in.changeMinimizerOrdering(newSplitter)
      }

      in = in.repartition(pbuckets.toOption.orElse(
        compatParams.map(_.buckets)).getOrElse(
        in.params.buckets))

      in.write(output())
      Index.read(output()).showStats()
    }
  }
  addSubcommand(reindex)
}