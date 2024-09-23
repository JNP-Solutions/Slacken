package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.ROOT
import org.apache.spark.sql.functions.{collect_list, count, lit, sum, udf, when}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql._

/** Various reports that describe the contents of an LCA to minimizer index. */
class IndexStatistics(index: KeyValueIndex)(implicit spark: SparkSession) {
  val taxonomy = index.taxonomy

  import spark.sqlContext.implicits._

  /**
   * Generates K-mer counts (with duplicates) for each taxon in the library and creates a TotalKmerCountReport
   *
   * @param indexBuckets
   * @param genomeLibrary
   * @return
   */
  def totalKmerCountReport(indexBuckets: DataFrame, genomeLibrary: GenomeLibrary): TotalKmerCountReport = {
    val k = index.k
    val spl = index.bcSplit

    val allTaxa = indexBuckets.groupBy("taxon").agg(count("*")).as[(Taxon, Long)].collect() //Dataframe

    val taxaLengthArray = index.joinSequencesAndLabels(genomeLibrary, addRC = false).map { x =>
      val superkmers = spl.value.superkmerPositions(x._2)
      val superkmerSum = superkmers.map(s => s.length - (k - 1)).sum
      (x._1, superkmerSum)
    }
      .toDF("taxon", "length").groupBy("taxon").agg(functions.sum($"length")).as[(Taxon, Long)].collect()

    new TotalKmerCountReport(taxonomy, allTaxa, taxaLengthArray)
  }

  /** For each genome in the input sequences, count all its minimizers (with repetitions) and calculate the fraction
   * that is assigned (in the index) to that genome's taxon, rather than some ancestor.
   * This is a measure of how well we can identify each distinct genome.
   *
   * @param indexBuckets index with LCAs
   * @param genomes      genome sequences to check (intended to be a subset of the sequences that were used
   *                     to build the index)
   */
  def showTaxonCoverageStats(indexBuckets: DataFrame, genomes: GenomeLibrary): Unit = {
    val inputSequences = index.joinSequencesAndLabels(genomes, addRC = false)
    val mins = index.findMinimizers(inputSequences)

    //1. Count how many times per input taxon each minimizer occurs
    val agg = mins.groupBy(index.idColumns :+ $"taxon": _*).agg(count("*").as("countAll"))
    // COLUMNS = [ taxon, minimizer(idColumns), countAll ]

    //2. Join with buckets, find the fraction that is assigned to the same (leaf) taxon
    val joint = agg.join(indexBuckets.withColumnRenamed("taxon", "idxTaxon"),
      // indexBuckets COLUMNS = [ idxTaxon, idColumnNames ]
      index.idColumnNames, "left").
      withColumn("countLeaf", when($"idxTaxon" === $"taxon", $"countAll").
        otherwise(lit(0L))).
      groupBy("taxon").
      agg((sum("countLeaf") / sum("countAll")).as("fracLeaf"),
        sum("countAll").as("total"))

    joint.select("fracLeaf", "total").summary().show()
  }

  /**
   *
   * @param indexBuckets
   * @param genomes
   * @return
   */
  def showTaxonFullCoverageStats(indexBuckets: DataFrame, genomes: GenomeLibrary): Array[(Taxon, Array[Int], Array[Long])] = {
    val inputSequences = index.joinSequencesAndLabels(genomes, addRC = false)
    val mins = index.findMinimizers(inputSequences)

    //1. Count how many times per input taxon each minimizer occurs
    val minCounts = mins.groupBy(index.idColumns :+ $"taxon": _*).agg(count("*").as("countAll"))

    val bcTaxonomy = index.bcTaxonomy
    val taxonDepth = udf((taxon: Taxon) => bcTaxonomy.value.depth(taxon))

    //2. Join with buckets, find the fraction that is assigned to the same (leaf) taxon
    minCounts.join(indexBuckets.withColumnRenamed("taxon", "idxTaxon"),
      index.idColumnNames). //[ taxon, LCA(idxtaxon) , Minimizer(idColumns), countAll ]
      withColumn("idxTaxDepth", taxonDepth($"idxtaxon")).
      groupBy("taxon", "idxTaxDepth").agg(sum($"countAll").as("countFull"))
      .groupBy("taxon").agg(collect_list($"idxTaxDepth").as("lcaDepths"), collect_list("countFull").as("counts"))
      .select("taxon", "lcaDepths", "counts").as[(Taxon, Array[Int], Array[Long])].collect()
  }
}

class TotalKmerCountReport(taxonomy: Taxonomy, counts: Array[(Taxon, Long)], val genomeSizes: Array[(Taxon, Long)])
  extends KrakenReport(taxonomy, counts) {

  lazy val totMinAgg = new TotalKmerSizeAggregator(taxonomy, genomeSizes)

  override def dataColumnHeaders: String =
    s"${super.dataColumnHeaders}\tTKC1-LeafOnly\tTKC2-FirstChildren\tTKC3-AllChildren"

  override def dataColumns(taxid: Taxon): String = {
    val totMinSizeS1 = math.round(totMinAgg.totKmerAverageS1(taxid))
    val totMinSizeS2 = math.round(totMinAgg.totKmerAverageS2(taxid))
    val totMinSizeS3 = math.round(totMinAgg.totKmerAverageS3(taxid))
    s"${super.dataColumns(taxid)}\t$totMinSizeS1\t$totMinSizeS2\t$totMinSizeS3"
  }
}

class TotalKmerSizeAggregator(taxonomy: Taxonomy, genomeSizes: Array[(Taxon, Long)]) {
  val genomeSizesMap = genomeSizes.toMap
  val computedTreeMap: mutable.Map[Taxon, (Long, Long)] = computeFullTree()

  /**
   * Average kmer count among all leaf-children of that taxon.
   * (present in the report under the column header "TKC1-LeafOnly")
   * @param taxon
   * @return
   */
  def totKmerAverageS1(taxon: Taxon): Double = {
    val s1Agg = taxonomy.children(taxon).map(child => computedTreeMap(child)).
      reduceOption((aggSum, pair) => (aggSum._1 + pair._1, aggSum._2 + pair._2))
      .getOrElse(computedTreeMap(taxon))

    val s1AggWithTaxon = if(genomeSizesMap.contains(taxon)) (s1Agg._1 + genomeSizesMap(taxon),s1Agg._2 + 1) else s1Agg

    s1AggWithTaxon._1.toDouble / s1AggWithTaxon._2.toDouble
  }

  /**
   * Average kmer count of average kmer counts of all first (immediate) children of that taxon.
   * (present in the report under the column header "TKC2-FirstChildren")
   * @param taxon
   * @return
   */
  def totKmerAverageS2(taxon: Taxon): Double = {
    if (taxonomy.children(taxon).nonEmpty) {
      val s2Agg = taxonomy.children(taxon).map(child => computedTreeMap(child)).filter(_._2 > 0)
        .map(pair => pair._1.toDouble / pair._2.toDouble)
      val s2AggWithTaxon = if(genomeSizesMap.contains(taxon)) genomeSizesMap(taxon).toDouble::s2Agg else s2Agg

      s2AggWithTaxon.sum / s2AggWithTaxon.size.toDouble
    } else {
      val a = computedTreeMap(taxon)
      if (a._2 == 0) 0 else a._1 / a._2
    }
  }

  /**
   * Average kmer count among all children of that taxon.
   * (present in the report under the column header "TKC3-AllChildren ")
   * @param taxon
   * @return
   */
  def totKmerAverageS3(taxon: Taxon): Double = {
    val childrenNonZero = taxonomy.children(taxon).map(child => computedTreeMap(child)).filter(_._2 > 0)
    val s1Agg = childrenNonZero.reduceOption { (aggSum, pair) => (aggSum._1 + pair._1, aggSum._2 + pair._2) }
      .getOrElse(computedTreeMap(taxon))
    val nonZeroChildSize = childrenNonZero.size.toDouble

    if (s1Agg._2 + nonZeroChildSize == 0) 0 else {
      ((totKmerAverageS1(taxon) * s1Agg._2.toDouble) +
        (totKmerAverageS2(taxon) * nonZeroChildSize)) / (s1Agg._2.toDouble + nonZeroChildSize)
    }
  }

  def computeFullTree(): mutable.Map[Taxon, (Long, Long)] = {
    val results = mutable.Map[Taxon, (Long, Long)]()
    computeLeafAggAndCounts(ROOT, results)
    results
  }

  def computeLeafAggAndCounts(taxon: Taxon, results: mutable.Map[Taxon, (Long, Long)]): (Long, Long) = {
    val children = taxonomy.children(taxon)
    children match {
      case Nil =>
        if (genomeSizesMap.contains(taxon)) {
          results += (taxon -> (genomeSizesMap(taxon), 1))
          (genomeSizesMap(taxon), 1)
        } else {
          results += (taxon -> (0, 0))
          (0, 0)
        }

      case childList =>
        val genomeSizes = ListBuffer[Long]()
        val genomeCounts = ListBuffer[Long]()
        if (genomeSizesMap.contains(taxon)) {
          genomeSizes += genomeSizesMap(taxon)
          genomeCounts += 1
        }
        for {i <- childList
             leafCounts = computeLeafAggAndCounts(i, results)} {
          genomeSizes += leafCounts._1
          genomeCounts += leafCounts._2
        }
        results += (taxon -> (genomeSizes.sum, genomeCounts.sum))
        (genomeSizes.sum, genomeCounts.sum)
    }
  }

}
