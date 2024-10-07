/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.{Inputs, NTSeq}
import org.apache.spark.sql.functions.{count, udf}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable


/** A library containing some number of genomes (or scaffolds etc) labelled with taxa.
 *
 * @param inputs Input genome sequence files
 * @param labelFile Path to a file labelling each sequence with a taxon (2-column TSV)
 */
final case class GenomeLibrary(inputs: Inputs, labelFile: String) {
  def taxonSet(taxonomy: Taxonomy)(implicit spark: SparkSession): mutable.BitSet = {
    import spark.sqlContext.implicits._

    //Collect a set of all taxa that have sequence in the library, and their ancestors
    //This (reading the labels file) is more efficient than reading the entire library and looking for distinct taxa,
    //but if the two diverge, then the label file will be taken as the source of truth here.
    val withSequence = mutable.BitSet.empty ++
      getTaxonLabels.map(_._2).distinct().collect()
    taxonomy.taxaWithAncestors(withSequence)
  }

  def getTaxonLabels(implicit spark: SparkSession): Dataset[(String, Taxon)] = {
    GenomeLibrary.getTaxonLabels(labelFile)
  }

  def joinSequencesAndLabels(addRC: Boolean)(implicit spark: SparkSession): Dataset[(Taxon, NTSeq)] = {
    import spark.sqlContext.implicits._

    val titlesTaxa = getTaxonLabels.toDF("header", "taxon")
    val idSeqDF = inputs.getInputFragments(addRC)
    idSeqDF.join(titlesTaxa, idSeqDF("header") === titlesTaxa("header")).
      select("taxon", "nucleotides").as[(Taxon, NTSeq)]
  }
}

object GenomeLibrary {
  val rankStrUdf = udf((x: Int) =>
    Taxonomy.rankValues.find(_.depth == x).map(_.title).getOrElse("???"))

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
      println(s"${invalidLabelledNodes.length} unknown genomes in $labelFile were not indexed (missing from the taxonomy):")
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

    val missingSteps = validLabelled.flatMap(x => tax.missingStepsToRoot(x)).toSeq.toDF("missingLevel")
    missingSteps.groupBy("missingLevel").agg(count("missingLevel")).sort("missingLevel").
      withColumn("label", rankStrUdf($"missingLevel")).
      show()
  }
}
