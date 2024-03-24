/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.hash.{InputFragment, MinSplitter, SpacedSeed}
import com.jnpersson.discount.spark.{AnyMinSplitter, HDFSUtil, IndexParams, Inputs}

import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.slacken.TaxonomicIndex.{ClassifiedRead, getTaxonLabels, sufficientHitGroups}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{count, desc, regexp_extract}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util


/** Parameters for classification of reads
 *
 * @param minHitGroups     min number of hit groups
 * @param withUnclassified whether to include unclassified reads in the output
 * @param thresholds       min. confidence scores (fraction of k-mers/minimizers that must be in the classified
 *                         taxon's clade)
 * @param sampleRegex      regular expression that identifies the sample ID of each read (for multi-sample mode).
 *                         e.g. ".*\\|(.*)\\|.*"
 *                         If none is specified, then single-sample mode is assumed.
 */
final case class ClassifyParams(minHitGroups: Int, withUnclassified: Boolean, thresholds: List[Double] = List(0.0),
                                sampleRegex: Option[String] = None)

/** Parameters for a Kraken1/2 compatible taxonomic index for read classification. Associates k-mers with LCA taxa.
 * @param params Parameters for k-mers, index bucketing and persistence
 * @param taxonomy The taxonomy
 * @tparam Record type of index records
 */
abstract class TaxonomicIndex[Record](params: IndexParams, taxonomy: Taxonomy)(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import spark.sqlContext.implicits._

  def split: AnyMinSplitter = bcSplit.value
  def bcSplit: Broadcast[AnyMinSplitter] = params.bcSplit
  def numIndexBuckets: Int = params.buckets
  def k: Int = split.k
  def m: Int = split.priorities.width

  lazy val bcTaxonomy = sc.broadcast(taxonomy)

  /** Sanity check input data */
  def checkInput(inputs: Inputs): Unit = {}

  /** Convenience function to make buckets with both trusted and untrusted (low quality) inputs.
   * @param trusted pair of (trusted  genomes, taxon label file)
   * @param untrusted optional pair of (untrusted genomes, taxon label file)
   * @param addRC whether to add reverse complements
   * @return index buckets
   */
  def makeBuckets(trusted:(Inputs, String), untrusted: Option[(Inputs, String)], addRC: Boolean): Dataset[Record] = {
    val (inputs, labels) = trusted
    untrusted match {
      case Some((ur, ulabels)) =>
        joinUntrustedBuckets(makeBuckets(inputs, labels, addRC),
          makeBuckets(ur, ulabels, addRC))
      case None =>
        makeBuckets(inputs, labels, addRC)
    }
  }

  /**
   * Construct buckets for a new index from genomes.
   *
   * @param reader           Input data
   * @param seqLabelLocation Location of a file that labels each genome with a taxon
   * @param addRC            Whether to add reverse complements
   * @return index buckets
   */
  def makeBuckets(reader: Inputs, seqLabelLocation: String, addRC: Boolean): Dataset[Record] = {
    val input = reader.getInputFragments(addRC).map(x =>
      (x.header, x.nucleotides))
    val seqLabels = getTaxonLabels(seqLabelLocation)
    makeBuckets(input, seqLabels)
  }

  /**
   * Build index buckets
   *
   * @param idsSequences Pairs of (genome title, genome)
   * @param taxonLabels  Pairs of (genome title, taxon)
   */
  def makeBuckets(idsSequences: Dataset[(SeqTitle, NTSeq)], taxonLabels: Dataset[(SeqTitle, Taxon)]): Dataset[Record]

  /** Join trusted buckets with untrusted (low quality) ones.
   * This is a left join. All trusted records will be kept.
   * The LCA function will be applied to pairs of trusted and untrusted values when both exist for a given minimizer.
   * The untrusted buckets will not be allowed to introduce new minimizers.
   * @param trusted Trusted minimizers
   * @param untrusted Untrusted minimizers
   * @return Combined minimizers
   */
  def joinUntrustedBuckets(trusted: Dataset[Record], untrusted: Dataset[Record]): Dataset[Record]

  def writeBuckets(buckets: Dataset[Record], location: String): Unit

  /** Respace this index to larger numbers of spaced seeds, creating a new index for
   * each value. This is possible because an index with s spaces contains all information necessary
   * to construct an index with s+x spaces (we effectively project it into the new space with some information loss)
   * Each new index will be written to a separate location.
   */
  def respaceMultiple(buckets: Dataset[Record], spaces: List[Int], outputLocation: String): Unit = {
    for {s <- spaces} {
      val (idx, bkts) = respace(buckets, s)
      val reg = "_s[0-9]+".r
      if (reg.findFirstIn(outputLocation).isEmpty) {
        throw new Exception(s"Unable to guess the correct output location for new indexes at: $outputLocation")
      }

      val outLoc = reg.replaceFirstIn(outputLocation, s"_s$s")
      idx.writeBuckets(bkts, outLoc)
      TaxonomicIndex.copyTaxonomy(params.location + "_taxonomy", outLoc + "_taxonomy")
    }
  }

  /** Remap this index to a larger number of spaces in the bit mask (irreversibly). */
  def respace(buckets: Dataset[Record], spaces: Int): (TaxonomicIndex[Record], Dataset[Record])

  /** Load index bucket from the params location */
  def loadBuckets(): Dataset[Record] =
    loadBuckets(params.location)

  /** Load index buckets from the specified location */
  def loadBuckets(location: String): Dataset[Record]

  /** Classify subject sequences using the index configured at the IndexParams location */
  def classify(buckets: Dataset[Record], subjects: Dataset[InputFragment],
               cpar: ClassifyParams): Dataset[(SeqTitle, Array[TaxonHit])]

  /** Classify subject sequences using the given index, optionally for multiple samples,
   * writing the results to a designated output location
   *
   * @param buckets        minimizer index
   * @param subjects       sequences to be classified
   * @param outputLocation location (directory, if multi-sample or prefix, if single sample) to write output
   * @param cpar           classification parameters
   */
  def classifyAndWrite(buckets: Dataset[Record], subjects: Dataset[InputFragment], outputLocation: String,
                       cpar: ClassifyParams): Unit = {
    if (cpar.thresholds.size == 1) {

      val t = cpar.thresholds.head
      val subjectsHits = classify(buckets, subjects, cpar)

      val classified = classifyForThreshold(subjectsHits, cpar, t)
      writeForSamples(classified, outputLocation, t, cpar)

    } else {
      //Multi-threshold mode
      //Cache taxon hits and then classify for multiple thresholds.
      //Amortizes the cost of generating taxon hits.
      val subjectsHits = classify(buckets, subjects, cpar)
        .cache()
      try {
        for {t <- cpar.thresholds} {
          val classified = classifyForThreshold(subjectsHits, cpar, t)
          writeForSamples(classified, outputLocation, t, cpar)
        }
      } finally {
        subjectsHits.unpersist()
      }
    }
  }

  /** Classify subject sequences using the index stored at the default location, optionally for multiple samples,
   * writing the results to a designated output location
   *
   * @param inputs         sequences to be classified
   * @param outputLocation (directory, if multi-sample or prefix, if single sample) to write output
   * @param cpar           classification parameters

   */
  def classifyAndWrite(inputs: Inputs, outputLocation: String, cpar: ClassifyParams): Unit = {
    val subjects = inputs.getInputFragments(withRC = false, withAmbiguous = true)
    classifyAndWrite(loadBuckets(), subjects, outputLocation, cpar)
  }

  /** Classify input sequence-hit dataset for a single sample and single confidence threshold value */
  def classifyForThreshold(subjectsHits: Dataset[(SeqTitle, Array[TaxonHit])],
                           cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val bcTax = this.bcTaxonomy
    val k = this.k
    val sre = cpar.sampleRegex.map(_.r)
    subjectsHits.map({ case (title, hits) =>
      val sortedHits = hits.sortBy(_.ordinal)

      val sufficientHits = sufficientHitGroups(sortedHits, cpar.minHitGroups)
      val summariesInOrder = TaxonCounts.concatenate(sortedHits.map(_.summary)) //TODO rewrite

      val sample = sre match {
        case Some(re) => re.findFirstMatchIn(title).
          map(_.group(1)).getOrElse("other")
        case _ => ""
      }

      TaxonomicIndex.classify(bcTax.value, sample, title, summariesInOrder, sufficientHits, threshold, k)
    })
  }

  /**
   * For each sample in the classified reads, write classified reads to a directory, with the _classified suffix,
   * as well as a kraken-style kreport.txt
   *
   * @param reads          classified reads
   * @param outputLocation directory/prefix to write to
   * @param threshold      the confidence threshold that was used in this classification
   * @param cpar           parameters for classification
   */
  private def writeForSamples(reads: Dataset[ClassifiedRead], outputLocation: String, threshold: Double, cpar: ClassifyParams): Unit = {
    val thresholds = cpar.thresholds
    // find the maximum number of digits after the decimal point for values in the threshold list
    // to enable proper sorting of file names with threshold values
    val maxDecimalLength = thresholds.map(num => num.toString.split("\\.")(1).length).max
    val thresholdStr = s"%.${maxDecimalLength}f".format(threshold)
    val location = outputLocation + "_c" + thresholdStr

    val keepLines = if (cpar.withUnclassified) {
      reads
    } else {
      reads.where($"classified" === true)
    }
    val outputRows = keepLines.map(r => (r.outputLine, r.sampleId)).
      toDF("classification", "sample")

    //These tables will be relatively small and we coalesce to avoid generating a lot of small files
    //in the case of an index with many partitions
    outputRows.coalesce(200).write.mode(SaveMode.Overwrite).
      partitionBy("sample").
      option("compression", "gzip").
      text(s"${location}_classified")
    makeReportsFromClassifications(s"${location}_classified")
  }

  /** For each subdirectory (corresponding to a sample), read back written classifications
   * and produce a KrakenReport. */
  private def makeReportsFromClassifications(location: String): Unit = {
    //At this point we don't have the sample IDs, so we have to explicitly traverse the filesystem
    //and look for the data that we wrote in the previous step
    for { d <- HDFSUtil.subdirectories(location) } {
      val loc = s"$location/$d"
      println(s"Generating Kraken report for $loc")
      val report = reportFromWrittenClassifications(loc)
      val sampleId = d.replaceFirst("sample=", "")
      HDFSUtil.usingWriter(s"$location/${sampleId}_kreport.txt", wr => report.print(wr))
    }
  }

  /** Read back written classifications from writeOutput to produce a KrakenReport. */
  private def reportFromWrittenClassifications(location: String): KrakenReport = {
    val countByTaxon = spark.read.option("sep", "\t").csv(location).
      map(x => x.getString(2).toInt).toDF("taxon").
      groupBy("taxon").agg(count("*").as("count")).
      sort(desc("count")).as[(Taxon, Long)].collect()
    new KrakenReport(bcTaxonomy.value, countByTaxon)
  }

  /** K-mers or minimizers in this index (keys) sorted by taxon depth from deep to shallow */
  def kmersDepths(buckets: Dataset[Record]): DataFrame

  /** Taxa in this index (values) together with their depths */
  def taxonDepths(buckets: Dataset[Record]): Dataset[(Taxon, Int)]

  def kmerDepthHistogram(): Dataset[(Int, Long)] = {
    val indexBuckets = loadBuckets()
    kmersDepths(indexBuckets).select("depth").groupBy("depth").count().
      sort("depth").as[(Int, Long)]
  }

  def taxonDepthHistogram(): Dataset[(Int, Long)] = {
    val indexBuckets = loadBuckets()
    taxonDepths(indexBuckets).select("depth").groupBy("depth").count().
      sort("depth").as[(Int, Long)]
  }

  /**
   * Write the histogram of this data to HDFS.
   * @param output Directory to write to (prefix name)
   */
  def writeDepthHistogram(output: String): Unit =
    kmerDepthHistogram().
      write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${output}_taxonDepths")
}

object TaxonomicIndex {

  /**
   * Read a taxon label file (TSV format)
   * Maps sequence id to taxon id.
   * This file is expected to be small (the data will be broadcast)
   * @param file Path to the file
   * @return
   */
  def getTaxonLabels(file: String)(implicit spark: SparkSession): Dataset[(String, Taxon)] = {
    import spark.sqlContext.implicits._
    spark.read.option("sep", "\t").csv(file).
      map(x => (x.getString(0), x.getString(1).toInt))
  }

  /** Show statistics for a taxon label file */
  def inputStats(labelFile: String, tax: Taxonomy)(implicit spark: SparkSession): Unit = {
    import spark.sqlContext.implicits._

    //Taxa from the taxon to genome mapping file
    val labelledNodes = getTaxonLabels(labelFile).select("_2").distinct().as[Taxon].collect()
    val invalidLabelledNodes = labelledNodes.filter(x => !tax.isDefined(x))
    if (invalidLabelledNodes.nonEmpty) {
      println(s"${invalidLabelledNodes.length} unknown genomes in $labelFile (missing from taxonomy):")
      println(invalidLabelledNodes.toList)
    }
    val nonLeafLabelled = labelledNodes.filter(x => !tax.isLeafNode(x))
    if (nonLeafLabelled.nonEmpty) {
      println(s"${nonLeafLabelled.length} non-leaf genomes in $labelFile")
//      println(nonLeafLabelled.toList)
    }

    val validLabelled = labelledNodes.filter(x => tax.isDefined(x))
    val max = tax.countDistinctTaxaWithAncestors(validLabelled)
    println(s"${validLabelled.length} valid taxa in input sequences described by $labelFile (maximal implied tree size $max)")
    println(s"Max leaf nodes in resulting database: ${validLabelled.length - nonLeafLabelled.length}")
  }

  /**
   * Read a taxonomy from a directory with NCBI nodes.dmp and names.dmp.
   * The files are expected to be small.
   * @return
   */
  def getTaxonomy(dir: String)(implicit spark: SparkSession): Taxonomy = {
    val nodes = HDFSUtil.getSource(s"$dir/nodes.dmp").
      getLines().map(_.split("\\|")).
      map(x => (x(0).trim.toInt, x(1).trim.toInt, x(2).trim))

    val names = HDFSUtil.getSource(s"$dir/names.dmp").
      getLines().map(_.split("\\|")).
      flatMap(x => {
        val nameType = x(3).trim
        if (nameType == "scientific name") {
          Some((x(0).trim.toInt, x(1).trim))
        } else None
      })

    Taxonomy.fromNodesAndNames(nodes.toArray, names)
  }

  //Copy a taxonomy to a new location (the files needed for classification and index access only.)
  //Possible optimisation: Remove data not actually used in the index.
  def copyTaxonomy(fromDir: String, toDir: String)(implicit spark: SparkSession): Unit = {
    HDFSUtil.copyFile(s"$fromDir/nodes.dmp", s"$toDir/nodes.dmp")
    HDFSUtil.copyFile(s"$fromDir/names.dmp", s"$toDir/names.dmp")
  }

  /** A classified read.
   * @param sampleId The ID of the sample (if available)
   * @param classified Could the read be classified?
   * @param title Sequence title/ID
   * @param taxon The assigned taxon
   * @param lengthString Length of the classified sequence
   * @param hitDetails Human-readable details for the hits
   */
  final case class ClassifiedRead(sampleId: String, classified: Boolean, title: SeqTitle, taxon: Taxon,
                                  lengthString: String, hitDetails: String) {
    def classifyFlag: String = if (!classified) "U" else "C"

    //Imitate the Kraken output format
    def outputLine: String = s"$classifyFlag\t$title\t$taxon\t$lengthString\t$hitDetails"
  }

  /** Classify a read.
   * @param taxonomy Parent map for taxa
   * @param title Sequence title/ID
   * @param summary Information about classified hit groups
   * @param sufficientHits Whether there are sufficient hits to classify the sequence
   * @param confidenceThreshold Minimum fraction of k-mers/minimizers that must be in the match (KeyValueIndex only)
   * @param k Length of k-mers
   */
  def classify(taxonomy: Taxonomy, sampleId: String, title: SeqTitle, summary: TaxonCounts,
               sufficientHits: Boolean, confidenceThreshold: Double, k: Int): ClassifiedRead = {
    val lca = new LowestCommonAncestor(taxonomy)
    val taxon = lca.resolveTree(summary, confidenceThreshold)
    val classified = taxon != Taxonomy.NONE && sufficientHits

    val reportTaxon = if (classified) taxon else Taxonomy.NONE
    ClassifiedRead(sampleId, classified, title, reportTaxon, summary.lengthString(k), summary.pairsInOrderString)
  }

  /** For the given set of sorted hits, was there a sufficient number of hit groups wrt the given minimum? */
  def sufficientHitGroups(sortedHits: Array[TaxonHit], minimum: Int): Boolean = {
    var hitCount = 0
    var lastMin = sortedHits(0).minimizer

    //count separate hit groups (adjacent but with different minimizers) for each sequence, imitating kraken2 classify.cc
    for { hit <- sortedHits } {
      if (hit.taxon != AMBIGUOUS_SPAN && hit.taxon != Taxonomy.NONE && hit.taxon != MATE_PAIR_BORDER &&
        (hitCount == 0 || !util.Arrays.equals(hit.minimizer, lastMin))) {
        hitCount += 1
      }
      lastMin = hit.minimizer
    }
    hitCount >= minimum
  }

}

