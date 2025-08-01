/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.input.{DirectInputs, PairedEnd, Ungrouped}
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers._
import com.jnpersson.slacken.Taxonomy.Species
import com.jnpersson.slacken.analysis.{MappingComparison, MinimizerMigration}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.exceptions.RequiredOptionNotFound
import org.rogach.scallop.{ScallopConf, Subcommand}

import java.io.FileNotFoundException
import java.util.regex.PatternSyntaxException


abstract class SparkCmd(title: String)(implicit val spark: SparkSession) extends RunCmd(title)
/** Command line options for commands that require an explicit taxonomy */
trait RequireTaxonomy {
  this: ScallopConf =>

  val taxonomy = opt[String](descr = "Path to taxonomy directory (nodes.dmp, merged.dmp and names.dmp)",
    required = true)
  def getTaxonomy(implicit spark: SparkSession) = Taxonomy.load(taxonomy())
}

/** Command line options for commands that require an input index */
trait RequireIndex {
  this: SparkCmd =>

  val index = opt[String](required = true, descr = "Location where the minimizer-LCA index is stored").
    map(l => HDFSUtil.makeQualified(l))

  def loadIndex() =
    KeyValueIndex.load(index(), getTaxonomy(index()))

  /** Get the Taxonomy from the index's default location */
  def getTaxonomy(indexLocation: String) =
    try {
      KeyValueIndex.getTaxonomy(indexLocation)
    } catch {
      case fnf: FileNotFoundException =>
        Console.err.println(s"Taxonomy not found: ${fnf.getMessage}.")
        throw fnf

    }
}

/** Command line options for commands that classify reads */
trait ClassifyCommand extends RequireIndex with HasInputReader {
  this: SparkCmd =>
  val minHitGroups = opt[Int](name = "min-hits", descr = "Minimum hit groups", default = Some(2))
  val inFiles = trailArg[List[String]](descr = "Sequences to be classified", default = Some(List()))
  val paired = opt[Boolean](descr = "Inputs are paired-end reads", default = Some(false)).map(
    if (_) PairedEnd else Ungrouped
  )
  val unclassified = toggle(descrYes = "Output unclassified reads", default = Some(true))
  val output = opt[String](descr = "Output location", required = true)
  val detailed = toggle(descrYes = "Output results for individual reads, in addition to reports", default = Some(true))
  val confidence = opt[List[Double]](
    descr = "Confidence thresholds (a space-separated list with values in [0, 1])",
    default = Some(List(0.0)), short = 'c')
  val sampleRegex = opt[String](descr = "Regular expression for extracting sample ID from read header (e.g. \"(.*)\\.\"). Enables multi-sample mode.")

  def cpar = ClassifyParams(minHitGroups(), unclassified(), confidence(), sampleRegex.toOption, detailed())

  validate(confidence) { cs =>
    cs.find(c => c < 0 || c > 1) match {
      case Some(c) => Left(s"--confidence values must be >= 0 and <= 1 ($c was given)")
      case None => Right(())
    }
  }

  validate(sampleRegex) { reg =>
    try {
      reg.r
      Right(())
    } catch {
      case pse: PatternSyntaxException =>
        println(pse.getMessage)
        Left(s"--sampleRegex was not a valid regular expression ($reg was given)")
    }
  }
}

/** Command line options for Slacken */
//noinspection TypeAnnotation
class SlackenConf(args: Seq[String])(implicit spark: SparkSession) extends SparkConfiguration(args) with HasInputReader {
  shortSubcommandsHelp(true)

  version(s"Slacken ${getClass.getPackage.getImplementationVersion} (c) 2019-2025 Johan Nyström-Persson")
  banner("Use <subcommand> --help to see help for a command. Use --detailed-help to see all options.")

  implicit val formats = SlackenMinimizerFormats

  override def defaultMaxSequenceLength: Int = 100000000 //100M bps

  /** Find genome library files (.fna) in a directory and construct a GenomeLibrary
   * @param location directory to search
   * @param k optionally override the default k-mer length
   */
  private def findGenomes(location: String, k: Int)(implicit spark: SparkSession): GenomeLibrary = {
    val inFiles = HDFSUtil.findFiles(location + "/library", ".fna")
    println(s"Discovered input files: $inFiles")
    val reader = inputReader(inFiles, k, Ungrouped)(spark)
    GenomeLibrary(reader, s"$location/seqid2taxid.map")
  }

  val build = new SparkCmd("build") with MinimizerCLIConf with RequireIndex with RequireTaxonomy {
    banner("Build a new index from genomes with taxa.")

    override def defaultK: Int = 35

    override def defaultMinimizerWidth: Int = 31

    override def defaultMinimizerSpaces: Int = 7

    override def defaultOrdering: String = "xor"

    override def defaultXORMask: Long = DEFAULT_TOGGLE_MASK

    override def canonicalMinimizers: Boolean = true

    override def frequencyBySequence: Boolean = true

    override def hasHiddenOptions: Boolean = true

    val library = opt[String](required = true, descr = "Location of genome library (directory containing library/)")

    val check = opt[Boolean](descr = "Only check input files for consistency", hidden = !showAllOpts,
      default = Some(false))

    def run(): Unit = {
      val genomes = findGenomes(library(), k())

      val params = IndexParams(
        spark.sparkContext.broadcast(
          SlackenMinimizerFormats.makeSplitter(this)), partitions(), index())
      println(s"Splitter ${params.splitter}")

      val i = new KeyValueIndex(spark.emptyDataFrame, params, getTaxonomy)

      if (check()) {
        i.checkInput(genomes.inputs)
      } else { //build index
        val recs = i.makeRecords(genomes)
        val ni = i.withRecords(recs)
        ni.writeRecords(params.location)
        Taxonomy.copyToLocation(taxonomy(), index() + "_taxonomy")

        val ni2 = loadIndex()
        ni2.showIndexStats(None)
        GenomeLibrary.inputStats(genomes.labelFile, getTaxonomy(index()))
      }
    }
  }
  addSubcommand(build)

  val respace = new SparkCmd("respace") with RequireIndex {
    banner("Efficiently build a new index from an existing one by increasing the number of spaces in the mask.")

    val output = opt[String](required = true, descr = "Output location")
    val spaces = opt[List[Int]](required = true, descr = "Numbers of spaces to generate indexes for")

    def run(): Unit = {
      val i = loadIndex()
      i.respaceMultiple(spaces(), output())
    }
  }
  addSubcommand(respace)

  val classify = new SparkCmd("classify") with ClassifyCommand {
    banner("Classify genomic sequences.")

    def run(): Unit = {
      val i = loadIndex()
      val inputs = inputReader(inFiles(), i.params.k, paired()).
      getInputFragments(true)
      val cls = new Classifier(i)
      cls.classifyAndWrite(inputs, output(), cpar)
    }
  }
  addSubcommand(classify)

  val classify2 = new SparkCmd("classify2") with ClassifyCommand {
    banner("Two-step classification using a static and a dynamic index (built on the fly).")

    override def hasHiddenOptions: Boolean = true

    val library = opt[String](required = true,
      descr = "Genome library location for index construction (directory containing library/)")

    val rank = choice(descr = "Granularity for index construction (default species)",
      default = Some(Species.title), choices = Taxonomy.rankTitles,
      hidden = !showAllOpts).map(Taxonomy.rankOrNull)

    val minCount = opt[Int](descr = "Minimizer count minimum", short = 'C',
      hidden = !showAllOpts)
    val minDistinct = opt[Int](descr = "Minimizer distinct count minimum", short = 'D',
      hidden = !showAllOpts)
    val reads = opt[Int](descr = "Min initial read count classified (default = 100)",
      short = 'R')
    val initConfidence = opt[Double](descr = "Confidence threshold for initial read classification",
      default = Some(0.15))

    val brackenLength = opt[Int](descr = "Read length for building bracken weights")

    val indexReports = opt[Boolean](descr = "Generate reports on the dynamic index and the inputs' taxon support",
      default = Some(false))

    val classifyWithGold = opt[Boolean](
      descr = "Instead of detecting taxa, construct a dynamic library using the gold taxon set ",
      default = Some(false))
    val goldSet = opt[String](descr = "Location of gold standard reference taxon set",
      short = 'g')
    val promoteGoldSet = choice(
      descr = "Attempt to promote taxa with no minimizers from the gold set to this rank (at the highest)",
      choices = Taxonomy.rankTitles,
      hidden = !showAllOpts).map(Taxonomy.rankOrNull)

    validate(initConfidence) { c =>
      if (c < 0 || c > 1)
        Left(s"--read-confidence must be >=0 and <= 1 ($c was given)")
      else Right(())
    }
    mutuallyExclusive(minCount, minDistinct, reads)

    override def run(): Unit = {
      val i = loadIndex()
      val genomeLib = findGenomes(library(), i.params.k)(i.spark)
      val goldStandardOpt = goldSet.toOption.map(x =>
        GoldSetOptions(x, promoteGoldSet.toOption, classifyWithGold()))
      val taxonCriteria = minCount.map(MinimizerTotalCount).
        orElse(reads.map(ClassifiedReadCount(_, initConfidence())).toOption).
        orElse(minDistinct.map(MinimizerDistinctCount).toOption).
        getOrElse(ClassifiedReadCount(100, initConfidence()))

      val dyn = new Dynamic(i, genomeLib, rank(),
        taxonCriteria,
        cpar,
        goldStandardOpt,
        output())(i.spark)

      val inputs = inputReader(inFiles(), i.params.k, paired())(i.spark)
      dyn.twoStepClassifyAndWrite(inputs, indexReports(), brackenLength.toOption)
    }
  }
  addSubcommand(classify2)

  val brackenBuild = new SparkCmd("bracken-build") with RequireIndex {
    banner("Generate a weights file (kmer_distrib) for use with Bracken.")

    val library = opt[String](descr = "Location of genome library (directory containing library/)")
    val readLen = opt[Int](descr = "Read length (default 100)", default = Some(100))

    def run(): Unit = {
      val i = loadIndex()
      val genomes = findGenomes(library(), readLen())
      val outputLocation = index() + "_bracken/database" + readLen() + "mers.kmer_distrib"

      val bw = new BrackenWeights(i, readLen())
      bw.buildAndWriteWeights(genomes, genomes.taxonSet(i.taxonomy), outputLocation, gradual = true)
    }
  }
  addSubcommand(brackenBuild)

  val stats = new SparkCmd("stats") with RequireIndex {
    banner("Get index statistics, optionally checking a genome library for coverage.")

    val library = opt[String](descr = "Location of genome library (directory containing library/) for coverage check")

    val histogram = opt[Boolean](descr = "Show taxonomic depth histograms for minimizers and taxa")

    def run(): Unit = {
      val i = loadIndex()
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

      if (histogram()) {
        println("Minimizer depth histogram")
        loadIndex().kmerDepthHistogram().show()
        println("Taxon depth histogram")
        loadIndex().taxonDepthHistogram().show()
      } else {
        val inputs = library.toOption.map(l => findGenomes(l, p.k))
        i.showIndexStats(inputs)
      }
    }
  }
  addSubcommand(stats)

  val inspect = new SparkCmd("inspect") with RequireIndex {
    banner("Generate an index contents report.")

    val library = opt[String](descr = "Location of genome library (directory containing library/)")
    val output = opt[String](descr = "Output location", required = true)
    val labels = opt[String](descr = "Labels file to check for missing nodes")

    def run(): Unit = {
      val idx = loadIndex()
      val genomes = library.toOption.map(lb => findGenomes(lb, idx.params.k)(idx.spark))
      idx.report(labels.toOption, output(), genomes)
    }
  }
  addSubcommand(inspect)

  val compareIndex = new SparkCmd("compareIndex") with RequireIndex {
    val reference = opt[String](descr = "Location of reference index", required = true)
    val output = opt[String](descr = "Output location", required = true)

    def run(): Unit = {
      val ref = KeyValueIndex.load(reference(), getTaxonomy(reference()))
      new MinimizerMigration(loadIndex(), ref).run(output())
    }
  }
  addSubcommand(compareIndex)

  val compare = new SparkCmd("compare") with RequireTaxonomy {
    banner("Compare classifications against a reference mapping.")
    val reference = opt[String](descr = "Reference mapping for comparison (TSV format)", required = true)
    val idCol = opt[Int](descr = "Read ID column in reference", default = Some(2))
    val taxonCol = opt[Int](descr = "Taxon column in reference", short = 'T', default = Some(3))
    val output = opt[String](descr = "Output location")
    val header = toggle(name = "header", descrYes = "Reference mapping has a header line", default = Some(false))

    val multiDirs = opt[List[String]](descr = "Directories of multi-sample mapping data to compare")
    val testFiles = opt[List[String]](descr = "Mapping files to compare")

    requireOne(multiDirs, testFiles)

    def run(): Unit = {
      val t = spark.sparkContext.broadcast(getTaxonomy)
      val mc = new MappingComparison(t, idCol(), taxonCol(), header(), 10, multiDirs.isDefined)
      if (testFiles.isDefined) {
        mc.processFiles(testFiles(), output(), reference())
      } else {
        mc.processDirectories(multiDirs(), output(), reference())
      }
    }
  }
  addSubcommand(compare)

  val inputCheck = new SparkCmd("inputCheck") with RequireTaxonomy {
    banner("Inspect input data.")
    val labels = opt[String](descr = "Path to sequence taxonomic label file")

    def run(): Unit = {
      val t = getTaxonomy
      for { l <- labels } {
        GenomeLibrary.inputStats (l, t)
      }
    }
  }
  addSubcommand(inputCheck)

  override protected def onError(e: Throwable): Unit = e match {
    case RequiredOptionNotFound(_) =>
      //Print help for the appropriate subcommand
      val cmds = subcommands.collect { case rc: RunCmd => rc }
      cmds match {
        case c :: cs =>
          c.builder.printHelp()
        case _ =>
          builder.printHelp()
      }
      super.onError(e)
    case _ => super.onError(e)
  }

}

/**
 * Slacken implements the Kraken 2 metagenomic classification algorithm on Spark,
 * while also improving on it and supporting a wider parameter space.
 * This is the main entry point for the Slacken Spark application.
 */
object Slacken extends SparkTool("Slacken") {
  def main(args: Array[String]): Unit = {
    try {
      val conf = new SlackenConf(args.toSeq)(sparkSession()).finishSetup()
      Commands.run(conf)
    } catch {
      case se: ScallopExitException =>
        handleScallopException(se)
    }
  }
}

/**
 * Functions for API/interactive use, as opposed to CLI.
 *
 * @param index         minimizer-LCA index
 * @param detailed      whether to generate detailed output (otherwise, only reports will be generated)
 * @param sampleRegex   regular expression to group reads by sample. Applied to read header to extract sample ID.
 * @param confidence    confidence score to classify for (the default value is 0)
 * @param minHitGroups  minimum number of hit groups (the default value is 2)
 * @param unclassified  whether to include unclassified reads in the result
 * @param spark
 * @return
 */
class Slacken(index: KeyValueIndex,
              detailed: Boolean,
              sampleRegex: Option[String],
              confidence: Double, minHitGroups: Int,
              unclassified: Boolean)(implicit spark: SparkSession) {

  /**
   * Location-based constructor.
   *
   * @param indexLocation HDFS location where the taxon-LCA index is stored
   * @param detailed      whether to generate detailed output (otherwise, only reports will be generated)
   * @param sampleRegex   regular expression to group reads by sample. Applied to read header to extract sample ID.
   * @param confidence    confidence score to classify for (the default value is 0)
   * @param minHitGroups  minimum number of hit groups (the default value is 2)
   * @param unclassified  whether to include unclassified reads in the result (false by default)
   * @param spark
   * @return
   */
  def this(indexLocation: String, detailed: Boolean,
           sampleRegex: Option[String],
           confidence: Double, minHitGroups: Int,
           unclassified: Boolean)(implicit spark: SparkSession) =
    this(KeyValueIndex.load(indexLocation), detailed, sampleRegex, confidence, minHitGroups, unclassified)

  if (confidence < 0 || confidence > 1) {
    throw new Exception(s"--confidence values must be >= 0 and <= 1 ($confidence was given)")
  }
  val cls = new Classifier(index)
  def cpar = ClassifyParams(minHitGroups, unclassified, List(confidence), sampleRegex, detailed)

  /**
   * Classify reads.
   *
   * @param reads reads to classify (R1 or singles)
   * @param reads2 optionally, R2 reads to classify in the case of paired-end reads.
   * @return a dataframe populated with [[ClassifiedRead]] objects.
   */
  def classifyReads(reads: DataFrame, reads2: Option[DataFrame] = None): DataFrame = {
    val inputs = reads2 match {
      case Some(r2) => DirectInputs.forPairs(reads, r2)
      case None => DirectInputs.forDataFrame(reads)
    }
    cls.classify(inputs.getInputFragments(true, None), cpar, confidence).toDF()
  }

  /**
   * Group reads by sample ID and write output files for each.
   *
   * @param classified a dataframe populated with [[ClassifiedRead]] objects.
   * @param location   location to write outputs to (directory prefix)
   * @return file names of generated report files
   */
  def writeReports(classified: DataFrame, location: String): Iterable[String] = {
    import spark.sqlContext.implicits._
    val clReads = classified.as[ClassifiedRead]
    val samples = cls.writePerSampleOutput(clReads, location, confidence, cpar)
    samples.map(s => Classifier.reportOutputLocation(location, s))
  }
}