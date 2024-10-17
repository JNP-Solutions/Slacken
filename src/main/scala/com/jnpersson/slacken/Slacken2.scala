/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.{Commands, HDFSUtil, IndexParams, RunCmd, SparkConfiguration, SparkTool}
import com.jnpersson.slacken.Taxonomy.Species
import com.jnpersson.slacken.analysis.{MappingComparison, Metrics}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopOption, Subcommand}

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
    case Some(l) => Taxonomy.load(l)
    case _ =>
      try {
        Taxonomy.load(s"${indexLocation}_taxonomy")
      } catch {
        case fnf: FileNotFoundException =>
          Console.err.println(s"Taxonomy not found: ${fnf.getMessage}. Please specify the taxonomy location with --taxonomy.")
          throw fnf
      }
  }

  private def findGenomes(location: String, k: Option[Int] = None)(implicit spark: SparkSession): GenomeLibrary = {
    val inFiles = HDFSUtil.findFiles(location + "/library", ".fna")
    println(s"Discovered input files: $inFiles")
    val reader = k match {
      case Some(k) => inputReader(inFiles, k, pairedEnd = false)
      case None => inputReader(inFiles)
    }
    GenomeLibrary(reader, s"$location/seqid2taxid.map")
  }

  val taxonIndex = new Subcommand("taxonIndex") {
    val location = trailArg[String](required = true, descr = "Path to location where index is stored")

    def index() =
      KeyValueIndex.load(location(), getTaxonomy(location()))

    val build = new RunCmd("build") {
      banner("Build a new index (library) from genomes")
      val library = opt[String](required = true, descr = "Location of sequence files (directory containing library/)")

      def run(): Unit = {
        val genomes = findGenomes(library())

        val params = IndexParams(
          spark.sparkContext.broadcast(
            MinSplitter(seedMask(minimizerConfig().getSplitter(Some(genomes.inputs.files)).priorities), k())
          ), partitions(), location())
        println(s"Splitter ${params.splitter}")

        val tax = getTaxonomy(location())
        val index = new KeyValueIndex(spark.emptyDataFrame, params, tax)

        val recs = index.makeRecords(genomes, addRC = false)
        val ni = index.withRecords(recs)
        ni.writeRecords(params.location)
        Taxonomy.copyToLocation(taxonomy(), location() + "_taxonomy")
        ni.showIndexStats(None)
        GenomeLibrary.inputStats(genomes.labelFile, tax)
      }
    }
    addSubcommand(build)

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

      val dynamic = opt[String](descr = "Library location for dynamic classification (if desired)")

      val dynamicRank = choice(descr = "Rank for initial classification in dynamic mode (default species)",
        default = Some(Species.title),
        choices = Taxonomy.rankValues.map(_.title)).map(r =>
        Taxonomy.rankValues.find(_.title == r).get)

//      val dynamicMinFraction = opt[Double](descr = "Min fraction for taxon inclusion in dynamic mode")
      val dynamicMinCount = opt[Int](descr = "Minimizer count for taxon inclusion in dynamic mode (default 100)")
      val dynamicMinDistinct = opt[Int](descr = "Minimizer distinct count for taxon inclusion in dynamic mode")
      val dynamicMinReads = opt[Int](descr = "Min read count classified for taxon inclusion in dynamic mode")
      val dynamicReadConfidence = opt[Double](descr = "Confidence threshold for initial read classification in dynamic mode (default 0.15)",
        default = Some(0.15))

      val dynamicBrackenLength = opt[Int](descr =
        "Read length for bracken weights for the dynamic index (default 100)", default = Some(100))

      val reportDynamicIndex = opt[Boolean](descr = "Report statistics on the dynamic index", default = Some(false))

      val classifyWithGoldStandard = opt[Boolean](descr = "whether to classify with the gold taxon set or just get " +
        "statistics wrt gold standard", default = Some(false), short = 'C')
      val goldStandardTaxonSet = opt[String](descr = "Location of gold standard reference taxon set in dynamic mode")
      val promoteGoldSet = choice(descr = "Attempt to promote taxa with no minimizers from the gold set to this rank (at the highest)",
        choices = Taxonomy.rankValues.map(_.title)).map(r =>
        Taxonomy.rankValues.find(_.title == r).get)

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

      mutuallyExclusive(dynamicMinCount, dynamicMinDistinct, dynamicMinReads)

      def run(): Unit = {
        val i = index()
        val inputs = inputReader(inFiles(), i.params.k, paired())

        dynamic.toOption match {
          case Some(library) =>
            val genomes = findGenomes(library, Some(i.params.k))
            val goldStandardOpt = goldStandardTaxonSet.toOption.map(x => (x, classifyWithGoldStandard(), promoteGoldSet.toOption))
            val taxonCriteria = dynamicMinCount.map(MinimizerTotalCount).
              orElse(dynamicMinReads.map(ClassifiedReadCount(_, dynamicReadConfidence())).toOption).
              orElse(dynamicMinDistinct.map(MinimizerDistinctCount).toOption).
              getOrElse(MinimizerTotalCount(100))

            val dyn = new Dynamic(i, genomes, dynamicRank(),
              taxonCriteria,
              cpar,
              dynamicBrackenLength.toOption, goldStandardOpt,
              reportDynamicIndex(),
              output())

            dyn.twoStepClassifyAndWrite(inputs, partitions())
          case None =>
            val cls = new Classifier(i)
            cls.classifyAndWrite(inputs, output(), cpar)
        }
      }
    }
    addSubcommand(classify)

    val brackenWeights = new RunCmd("brackenWeights") {
      banner("Generate a weights file (kmer_distrib) for use with Bracken")

      val library = opt[String](descr = "Location of sequence files (directory containing library/)")
      val readLen = opt[Int](descr = "Read length (default 100)", default = Some(100))

      def run(): Unit = {
        val i = index()
        val genomes = findGenomes(library(), Some(readLen()))
        val outputLocation = location() + "_bracken/database" + readLen() + "mers.kmer_distrib"

        val bw = new BrackenWeights(i, readLen())
        bw.buildAndWriteWeights(genomes, genomes.taxonSet(i.taxonomy), outputLocation, gradual = true)
      }
    }
    addSubcommand(brackenWeights)

    val stats = new RunCmd("stats") {
      banner("Get index statistics (optionally referencing input sequences)")

      val library = opt[String](descr = "Location of sequence files (directory containing library/) for coverage check")

      def run(): Unit = {
        val i = index()
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
        val inputs = library.toOption.map(l => findGenomes(l, Some(p.k)))
        i.showIndexStats(inputs)
      }
    }
    addSubcommand(stats)

    val histogram = new RunCmd("histogram") {
      banner("Get index statistics as a histogram")

//      val output = opt[String](descr = "Output location", required = true) //TODO
      def run(): Unit = {
        println("Minimizer depths")
        index().kmerDepthHistogram().show()
        println("Taxon depths")
        index().taxonDepthHistogram().show()
      }
    }
    addSubcommand(histogram)

    val report = new RunCmd("report") {
      banner("Generate an index contents report")

      val library = opt[String](descr = "Location of sequence files (directory containing library/)")
      val output = opt[String](descr = "Output location", required = true)
      val labels = opt[String](descr = "Labels file to check for missing nodes")

      def run(): Unit = {
        val genomes: Option[GenomeLibrary] = library.toOption match {
          case Some(lb) =>
            Some(findGenomes(lb))
          case None =>
            None
        }
        index().report(labels.toOption, output(), genomes)
      }
    }
    addSubcommand(report)
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
      val t = spark.sparkContext.broadcast(Taxonomy.load(taxonomy()))
      val mc = new MappingComparison(t, reference(), idCol(), taxonCol(), skipHeader(), 10, multi())
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
        GenomeLibrary.inputStats (l, t)
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
