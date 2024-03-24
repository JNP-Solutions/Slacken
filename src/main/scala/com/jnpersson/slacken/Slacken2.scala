/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.hash.{DEFAULT_TOGGLE_MASK, MinSplitter, RandomXOR, SpacedSeed}
import com.jnpersson.discount.spark.{Commands, Configuration, HDFSUtil, IndexParams, RunCmd, SparkConfiguration, SparkTool}
import com.jnpersson.slacken.analysis.{MappingComparison, Metrics, MinimizerMigration}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand

import java.io.FileNotFoundException
import java.util.regex.PatternSyntaxException

//noinspection TypeAnnotation
class Slacken2Conf(args: Array[String])(implicit spark: SparkSession) extends SparkConfiguration(args) {
  version(s"Slacken 2 ${getClass.getPackage.getImplementationVersion} beta (c) 2019-2023 Johan Nyström-Persson")
  banner("Usage:")

  val taxonomy = opt[String](descr = "Path to taxonomy directory (nodes.dmp and names.dmp)")

  override def defaultMinimizerSpaces: Int = 7
  override def defaultOrdering: String = "xor"
  override def defaultAllMinimizers: Boolean = true
  override def defaultMaxSequenceLength: Int = 100000000 //100M bps

  override def defaultXORMask: Long = DEFAULT_TOGGLE_MASK
  override def canonicalMinimizers: Boolean = true
  override def frequencyBySequence: Boolean = true

  /** Get the Taxonomy from the default location or from the user-overridden location */
  def getTaxonomy(indexLocation: String) = taxonomy.toOption match {
    case Some(l) => TaxonomicIndex.getTaxonomy(l)
    case _ =>
      try {
        TaxonomicIndex.getTaxonomy(s"${indexLocation}_taxonomy")
      } catch {
        case fnf: FileNotFoundException =>
          Console.err.println(s"Taxonomy not found: ${fnf.getMessage}. Please specify the taxonomy location with --taxonomy.")
          throw fnf
      }
  }

  private def findInputs(location: String) = {
    val inFiles = HDFSUtil.findFiles(location + "/library", ".fna")
    println(s"Discovered input files: $inFiles")
    val reader = inputReader(inFiles)
    (reader, s"$location/seqid2taxid.map")
  }

  val taxonIndex = new Subcommand("taxonIndex") {
    val location = trailArg[String](required = true, descr = "Path to location where index is stored")

    def index() =
      KeyValueIndex.load(location(), getTaxonomy(location()))

    val build = new RunCmd("build") {
      banner("Build a new index (library) from genomes")
      val library = opt[String](required = true, descr = "Location of sequence files (directory containing library/)")
      val untrusted = opt[String](descr = "Location of untrusted (low quality) input sequence files")
      val check = opt[Boolean](descr = "Only check input files for consistency", hidden = true, default = Some(false))

      def run(): Unit = {
        val tInputs = findInputs(library())
        val uInputs = untrusted.toOption.map(u => findInputs(u))

        val params = IndexParams(
          spark.sparkContext.broadcast(
            MinSplitter(seedMask(discount.getSplitter(Some(tInputs._1.files)).priorities), k())
          ), partitions(), location())
        println(s"Splitter ${params.splitter}")

        val tax = getTaxonomy(location())
        val index = new KeyValueIndex(params, tax)

        if (check()) {
          index.checkInput(tInputs._1)
        } else { //build index
          val bkts = index.makeBuckets(tInputs, uInputs, addRC = false)
          index.writeBuckets(bkts, params.location)
          TaxonomicIndex.copyTaxonomy(taxonomy(), location() + "_taxonomy")
          index.showIndexStats()
          TaxonomicIndex.inputStats(tInputs._2, tax)
        }
      }
    }
    addSubcommand(build)

    val respace = new RunCmd("respace") {
      banner("Build a new index from an existing one by increasing the number of spaces in the mask")

      val output = opt[String](required = true, descr = "Output location")
      val spaces = opt[List[Int]](required = true, descr = "Numbers of spaces to generate indexes for")

      def run(): Unit = {
        val i = index
        i.respaceMultiple(i.loadBuckets(), spaces(), output())
      }
    }
    addSubcommand(respace)

    val classify = new RunCmd("classify") {
      banner("Classify genomic sequences")

      val minHitGroups = opt[Int](name = "minHits", descr = "Minimum hit groups (default 2)", default = Some(2))
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val paired = opt[Boolean](descr = "Inputs are paired-end reads", default = Some(false))
      val unclassified = toggle(descrYes = "Output unclassified reads", default = Some(true))
      val output = opt[String](descr = "Output location", required = true)
      val confidence = opt[List[Double]](
        descr = "Confidence thresholds (default 0.0, should be a space separated list with values in [0, 1])",
        default = Some(List(0.0)), short = 'c')
      val sampleRegex = opt[String](descr = "Regular expression for extracting sample ID from read header (e.g. \"@(.*):\")")
      def cpar = ClassifyParams(minHitGroups(), unclassified(), confidence(), sampleRegex.toOption)

      validate (confidence) { cs =>
        cs.find(c => c < 0 || c > 1) match {
          case Some(c) => Left(s"--confidence values must be >= 0 and <= 1 ($c was given)")
          case None => Right(Unit)
        }
      }

      validate(sampleRegex) { reg =>
        try {
          reg.r
          Right(Unit)
        } catch {
          case pse: PatternSyntaxException =>
            println(pse.getMessage)
            Left(s"--sampleRegex was not a valid regular expression ($reg was given)")
        }
      }

      def run(): Unit = {
        val i = index
        val inputs = inputReader(inFiles(), i.params.k, paired())
        i.classifyAndWrite(inputs, output(), cpar)
      }
    }
    addSubcommand(classify)

    val stats = new RunCmd("stats") {
      banner("Get index statistics")

      def run(): Unit = {
        val i = index
        val p = i.params
        p.splitter.priorities match {
          case ss@SpacedSeed(_, inner) =>
            println("Spaced mask (left aligned) " + ss.spaceMask.toBinaryString)
            inner match {
              case rx@RandomXOR(_, _, _) =>
                println("Toggle mask (left aligned) " + rx.mask.toBinaryString)
              case _ =>
            }
            println(s"Inner splitter $inner")
          case _ =>
            println(s"Splitter ${p.splitter}")
        }
        i.showIndexStats()
      }
    }
    addSubcommand(stats)

    val histogram = new RunCmd("histogram") {
      banner("Get index statistics as a histogram")

//      val output = opt[String](descr = "Output location", required = true) //TODO
      def run(): Unit = {
        println("Minimizer depths")
        index.kmerDepthHistogram().show()
        println("Taxon depths")
        index.taxonDepthHistogram().show()
      }
    }
    addSubcommand(histogram)

    val report = new RunCmd("report") {
      banner("Generate an index contents report")

      val output = opt[String](descr = "Output location", required = true)
      val labels = opt[String](descr = "Labels file to check for missing nodes")

      def run(): Unit = {
        index.report(labels.toOption, output())
      }
    }
    addSubcommand(report)

    def compare = new RunCmd("compare") {
      val reference = opt[String](descr = "Location of reference index", required = true)
      val output = opt[String](descr = "Output location", required = true)

      def run(): Unit = {
        val ref = KeyValueIndex.load(reference(), getTaxonomy(reference()))
        new MinimizerMigration(index, ref).run(output())
      }
    }
    addSubcommand(compare)
  }
  addSubcommand(taxonIndex)

  val compare = new RunCmd("compare") {
    banner("Compare classifications")
    val reference = opt[String](descr = "Reference mapping to compare (TSV format)", required = true)
    val idCol = opt[Int](descr = "Read ID column in reference", default = Some(2))
    val taxonCol = opt[Int](descr = "Taxon column in reference", short = 'T', default = Some(3))
    val output = opt[String](descr = "Output location")
    val skipHeader = toggle(name = "header", descrYes = "Skip header in reference data", default = Some(false))
    val multi = toggle(name = "multi", descrYes = "Classified reads were classified as multi-sample data", default = Some(true))

    val testFiles = trailArg[List[String]]("testFiles", descr = "Mappings to compare (Slacken/Kraken format)",
      required = true)

    def run(): Unit = {
      val t = spark.sparkContext.broadcast(TaxonomicIndex.getTaxonomy(taxonomy()))
      val mc = new MappingComparison(t, reference(), idCol(), taxonCol(), skipHeader(), 100, multi())
      val metrics =
        for { t <- testFiles().iterator
            m <- mc.allMetrics(t) } yield m
      HDFSUtil.writeTextLines(output() + "_metrics.tsv",
        Iterator(Metrics.header) ++ metrics.flatMap(_.toTSVString))
    }
  }
  addSubcommand(compare)

  val inputCheck = new RunCmd("inputCheck") {
    banner("Inspect input data")
    val labels = opt[String](descr = "Path to sequence taxonomic label file")

    def run(): Unit = {
      val t = getTaxonomy(taxonomy())
      for { l <- labels } {
        TaxonomicIndex.inputStats (l, t)
      }
    }
  }
  addSubcommand(inputCheck)

  verify()
}

/** Implements the Kraken 2 method for taxonomic classification. */
object Slacken2 extends SparkTool("Slacken 2") {
  def main(args: Array[String]): Unit = {
    val conf = new Slacken2Conf(args)(sparkSession()).finishSetup()
    Commands.run(conf)
  }
}