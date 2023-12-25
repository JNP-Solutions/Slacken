/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.hash.{DEFAULT_TOGGLE_MASK, Extended, MinSplitter, RandomXOR, SpacedSeed}
import com.jnpersson.discount.spark.{All, AnyMinSplitter, Commands, Configuration, Discount, Generated,
  IndexParams, MinimizerSource, RunCmd, SparkTool}
import com.jnpersson.discount.{Frequency, Given}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand

class Slacken2Conf(args: Array[String]) extends Configuration(args) {
  version(s"Slacken 2 ${getClass.getPackage.getImplementationVersion} beta (c) 2019-2023 Johan Nyström-Persson")
  banner("Usage:")

  val minHitGroups = opt[Int](name = "minHits", descr = "Minimum hit groups (default 2)", default = Some(2))

  override def defaultMinimizerSpaces: Int = 7
  override def defaultOrdering: String = "xor"
  override def defaultAllMinimizers: Boolean = true
  override def defaultMaxSequenceLength: Int = 100000000 //100M bps

  override def defaultXORMask: Long = DEFAULT_TOGGLE_MASK
  override def canonicalMinimizers: Boolean = true
  override def frequencyBySequence: Boolean = true

  def minimizerOrderingByTaxonDepth(inFiles: List[String], taxonomyLocation: String,
                                    seqLabelLocation: String)(implicit spark: SparkSession): MinimizerSource = {
    val innerM = minimizerWidth() - 4
    assert(innerM >= 1)
    assert(minimizerWidth() <= 15)

    val d = discount.copy(k = minimizerWidth(), minimizers = All, m = innerM, normalize = true)
    //Construct a temporary Slacken 1 style m-mer index to create a minimizer ordering for Slacken 2.
    //This will determine the taxon depth of each m-mer. The minimizer ordering of the inner Slacken 1 index
    //is not biologically significant.

    val idx = SupermerIndex.empty(d, taxonomyLocation, inFiles, None)
    val bkts = idx.makeBuckets(d, inFiles, seqLabelLocation, addRC = true)
    Generated(idx.minimizerDepthOrdering(bkts, complete = true))
  }

  /** Configure a splitter for a new index */
  def configureNewSplitter(inFiles: Option[List[String]], taxonomyLocation: String,
                           seqLabelLocation: String)(implicit spark: SparkSession): AnyMinSplitter = {
    validateMAndKOptions()

    val (ord, minSource) = ordering() match {
      case Frequency(true) =>
        //Construct the special taxon depth-based minimizer ordering
        val inner = minimizerOrderingByTaxonDepth(inFiles.getOrElse(List()), taxonomyLocation, seqLabelLocation)
        (Given, extendMinimizersIfConfigured(inner))
      case _ => (ordering(), parseMinimizerSource)
    }

    //Always normalize sequences, in case we are constructing a frequency table for the splitter
    val d = discount.copy(minimizers = minSource, ordering = ord, normalize = true)
    MinSplitter(seedMask(d.getSplitter(inFiles).priorities), k())
  }

  val taxonIndex = new Subcommand("taxonIndex") {
    val location = opt[String](required = true, descr = "Path to location where index is stored")
    val taxonomy = opt[String](descr = "Path to taxonomy directory (nodes.dmp and names.dmp)", required = true)

    def index(implicit spark: SparkSession) =
      KeyValueIndex.load(location(), taxonomy())

    val build = new RunCmd("build") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val labels = opt[String](required = true, descr = "Path to sequence taxonomic label file")

      def run(implicit spark: SparkSession): Unit = {
        val d = discount

        val params = IndexParams(
          spark.sparkContext.broadcast(
            configureNewSplitter(inFiles.toOption, taxonomy(), labels())
          ), partitions(), location())
        println(s"Splitter ${params.splitter}")
        val index = new KeyValueIndex(params, TaxonomicIndex.getTaxonomy(taxonomy()))
        val bkts = index.makeBuckets(d, inFiles(), labels(), addRC = false)
        index.writeBuckets(bkts, params.location)
      }
    }
    addSubcommand(build)

    val rebucket = new RunCmd("rebucket") {
      val outputLocation = opt[String](required = true, descr = "Path to write rebucketed index to")
      override def run(implicit spark: SparkSession): Unit = {
        //TODO implement functionality

//        val i = index
//        val bkts = i.loadIndex()
//        i.writeIndex(bkts, outputLocation())
      }
    }
    addSubcommand(rebucket)

    val union = new RunCmd("union") {
      val indexes = trailArg[List[String]](required = true, descr = "Indexes to union with")
      val outputLocation = opt[String](required = true, descr = "Path to write union index to")
      override def run(implicit spark: SparkSession): Unit = {
        val unionIndexes = indexes()
        index.unionIndexes(location() :: unionIndexes, outputLocation())
      }
    }
    addSubcommand(union)

    val classify = new RunCmd("classify") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val paired = opt[Boolean](descr = "Inputs are paired-end reads", default = Some(false))
      val unclassified = toggle(descrYes = "Output unclassified reads", default = Some(true))
      val output = opt[String](descr = "Output location", required = true)

      def run(implicit spark: SparkSession): Unit = {
        val i = index
        val d = discount(i.params)
        val input = d.inputReader(paired(), inFiles(): _*).getInputFragments(withRC = false, withAmbiguous = true)
        i.classifyAndWrite(input, output(), unclassified(), minHitGroups())
      }
    }
    addSubcommand(classify)

    val stats = new RunCmd("stats") {
      def run(implicit spark: SparkSession): Unit = {
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

    def histogram = new RunCmd("histogram") {
//      val output = opt[String](descr = "Output location", required = true) //TODO
      def run(implicit spark: SparkSession): Unit = {
        index.depthHistogram().show()
      }
    }
    addSubcommand(histogram)

  }
  addSubcommand(taxonIndex)

  val compare = new RunCmd("compare") {
    val taxonomy = opt[String](descr = "Path to taxonomy directory (nodes.dmp and names.dmp)", short = 't',
      required = true)
    val reference = opt[String](descr = "Reference mapping to compare (TSV format)", required = true)
    val idCol = opt[Int](descr = "Read ID column in reference", default = Some(2))
    val taxonCol = opt[Int](descr = "Taxon column in reference", short = 'T', default = Some(3))
    val output = opt[String](descr = "Output location", required = true) //TODO implement this
    val skipHeader = toggle(name = "header", descrYes = "Skip header in reference data", default = Some(false))

    val testFiles = trailArg[List[String]]("testFiles", descr = "Mappings to compare (Slacken/Kraken format)",
      required = true)

    def run(implicit spark: SparkSession): Unit = {
      val t = spark.sparkContext.broadcast(TaxonomicIndex.getTaxonomy(taxonomy()))
      val mc = new MappingComparison(t, reference(), idCol(), taxonCol(), skipHeader())
      for { t <- testFiles() } {
        println(t)
        mc.compare(t)
      }
    }
  }
  addSubcommand(compare)

  verify()
}

/** Implements the Kraken 2 method for taxonomic classification. */
object Slacken2 extends SparkTool("Slacken 2") {
  def main(args: Array[String]): Unit = {
    val conf = new Slacken2Conf(args)
    Commands.run(conf)(sparkSession(conf))
  }
}