/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.hash.{DEFAULT_TOGGLE_MASK, MinSplitter, RandomXOR, SpacedSeed}
import com.jnpersson.discount.spark.{Commands, Configuration, HDFSUtil, IndexParams, RunCmd, SparkTool}
import com.jnpersson.slacken.analysis.{MappingComparison, Metrics}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand

import java.io.FileNotFoundException

class Slacken2Conf(args: Array[String]) extends Configuration(args) {
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
  def getTaxonomy(indexLocation: String)(implicit spark: SparkSession) = taxonomy.toOption match {
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

  val taxonIndex = new Subcommand("taxonIndex") {
    val location = trailArg[String](required = true, descr = "Path to location where index is stored")

    def index(implicit spark: SparkSession) =
      KeyValueIndex.load(location(), getTaxonomy(location()))

    val build = new RunCmd("build") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val labels = opt[String](required = true, descr = "Path to sequence taxonomic label file")
      val all = toggle(descrYes = "Require minimizers to be present in all LCA genomes", default = Some(false))

      def run(implicit spark: SparkSession): Unit = {
        val d = discount

        val params = IndexParams(
          spark.sparkContext.broadcast(
            MinSplitter(seedMask(discount.getSplitter(inFiles.toOption).priorities), k())
          ), partitions(), location())
        println(s"Splitter ${params.splitter}")

        val method = if (all()) LCARequireAll else LCAAtLeastTwo
        val tax = getTaxonomy(location())
        val index = new KeyValueIndex(params, tax)

        val bkts = index.makeBuckets(d, inFiles(), labels(), addRC = false, method)
        index.writeBuckets(bkts, params.location)
        TaxonomicIndex.copyTaxonomy(taxonomy(), location() + "_taxonomy")
        index.showIndexStats()
        TaxonomicIndex.inputStats(labels(), tax)
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
      val minHitGroups = opt[Int](name = "minHits", descr = "Minimum hit groups (default 2)", default = Some(2))
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val paired = opt[Boolean](descr = "Inputs are paired-end reads", default = Some(false))
      val unclassified = toggle(descrYes = "Output unclassified reads", default = Some(true))
      val output = opt[String](descr = "Output location", required = true)
      val confidence = opt[Double](descr = "Confidence score (default 0, should be in [0, 1])", default = Some(0))
      def cpar = ClassifyParams(minHitGroups(), confidence(), unclassified())

      def run(implicit spark: SparkSession): Unit = {
        val i = index
        val d = discount(i.params)
        val input = d.inputReader(paired(), inFiles(): _*).getInputFragments(withRC = false, withAmbiguous = true)
        i.classifyAndWrite(input, output(), cpar)
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
        println("Minimizer depths")
        index.kmerDepthHistogram().show()
        println("Taxon depths")
        index.taxonDepthHistogram().show()
      }
    }
    addSubcommand(histogram)

  }
  addSubcommand(taxonIndex)

  val compare = new RunCmd("compare") {
    val reference = opt[String](descr = "Reference mapping to compare (TSV format)", required = true)
    val idCol = opt[Int](descr = "Read ID column in reference", default = Some(2))
    val taxonCol = opt[Int](descr = "Taxon column in reference", short = 'T', default = Some(3))
    val output = opt[String](descr = "Output location")
    val skipHeader = toggle(name = "header", descrYes = "Skip header in reference data", default = Some(false))

    val testFiles = trailArg[List[String]]("testFiles", descr = "Mappings to compare (Slacken/Kraken format)",
      required = true)

    def run(implicit spark: SparkSession): Unit = {

      val t = spark.sparkContext.broadcast(TaxonomicIndex.getTaxonomy(taxonomy()))
      val mc = new MappingComparison(t, reference(), idCol(), taxonCol(), skipHeader(), 100)
      val metrics =
        for { t <- testFiles().iterator
            m <- mc.allMetrics(t) } yield m
      HDFSUtil.writeTextLines(output() + "_metrics.tsv",
        Iterator(Metrics.header) ++ metrics.map(_.toTSVString))
    }
  }
  addSubcommand(compare)

  val inputCheck = new RunCmd("inputCheck") {
    val labels = opt[String](required = true, descr = "Path to sequence taxonomic label file")

    def run(implicit spark: SparkSession): Unit = {
      val t = getTaxonomy(taxonomy())
      TaxonomicIndex.inputStats(labels(), t)
    }

  }
  addSubcommand(inputCheck)

  verify()
}

/** Implements the Kraken 2 method for taxonomic classification. */
object Slacken2 extends SparkTool("Slacken 2") {
  def main(args: Array[String]): Unit = {
    val conf = new Slacken2Conf(args)
    Commands.run(conf)(sparkSession(conf))
  }
}