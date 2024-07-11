/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.kmers
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.{IndexParams, Inputs, TestGenerators, Testing => TTesting}
import com.jnpersson.slacken.Taxonomy.{NONE, ROOT, Rank, Root}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalacheck.{Gen, Shrink}

object Testing {

  //Construct a smaller taxonomy by removing a single node
  private def withoutNode(tax: Taxonomy, n: Taxon): Taxonomy = {
    val parents = tax.parents.clone()
    //setting parents to NONE is enough to mark the taxon as unused.
    //there is no need to also remove it from ranks and scientific names.
    parents(n) = NONE
    for { c <- tax.children(n) } {
      parents(c) = ROOT
    }
    Taxonomy(parents, tax.ranks, tax.scientificNames)
  }

  implicit def shrinkTaxonomy: Shrink[Taxonomy] = Shrink {
    tax =>
      tax.taxa.filter(x => x != ROOT).
        toStream.map(i => withoutNode(tax, i))
  }

  /** Generate a taxon with a parent in the ID range 1..maxParent */
  private def taxonAtLevel(n: Taxon, rank: Rank, maxParent: Taxon) =
    for { parent <- Gen.choose(1, maxParent) }
      yield (n, parent, rank.title)

  /** Generator for taxonomies of a given size (number of nodes).
   * An equal number of nodes at every taxonomic rank will be generated.
   * Nodes at higher levels will have lower IDs than at lower levels.
   */
  def taxonomies(size: Int): Gen[Taxonomy] = {
    import org.scalacheck.util.Buildable.buildableSeq

    val levelSize = size / 10
    //Generate data for every level (rank) in the tree and then compose them
    val levelGenerators: Gen[Seq[(Taxon, Taxon, String)]] =
      Gen.sequence(for {
      rank <- Taxonomy.rankValues
      if rank != Taxonomy.Root
      subLevel <- Seq(1, 2) //simulate e.g. S1, S2
      maxParent = (rank.depth - 1) * levelSize + 1 //each level has a fixed ID range
      id <- ((rank.depth - 1) * levelSize + 2) until (rank.depth * levelSize + 2)
    } yield taxonAtLevel(id, rank, maxParent))

    val rootNode = (1, 1, Root.title)
    levelGenerators.map(nodes => {
//      println(nodes)
      val all = (rootNode +: nodes).toArray
      Taxonomy.fromNodesAndNames(all, all.iterator.map(n => (n._1, s"Taxon ${n._1}")))
    })
  }

  def extendedTable(e: Int, m: Int): Gen[ExtendedTable] = {
    val inner = TTesting.minTable(m)
    for { canonical <- Gen.oneOf(true, false)
          withSuf <- Gen.oneOf(true, false) }
    yield ExtendedTable(inner, e, canonical, withSuf)
  }

  def minimizerPriorities(m: Int): Gen[MinimizerPriorities] = {
    if (m >= 30) {
      //ExtendedTable only works for somewhat large m
      Gen.oneOf(TestGenerators.minimizerPriorities(m), extendedTable(m, 10))
    } else {
      kmers.TestGenerators.minimizerPriorities(m)
    }
  }

}

object TestData {
  /**
   * A hardcoded taxonomy for the tiny test dataset in testData/slacken/slacken_tinydata.fna.
   * Make both strains direct children of root as a simple way to generate test data.
   */
  def taxonomy =
    Taxonomy.fromNodesAndNames(
      Array((455631, ROOT, "strain"),
        (526997, ROOT, "strain")),
      Iterator((455631, "Clostridioides difficile QCD-66c26"),
        (526997, "Bacillus mycoides DSM 2048"))
    )

  //Number of length 100 reads for each taxon (including masked or invalid regions).
  //Manually calculated from the .fai files
  val numberOfLength100Reads = List((455631, 4126265L), (526997, 3070413L))

  def inputs(k: Int)(implicit spark: SparkSession) =
    new Inputs(List("testData/slacken/slacken_tinydata.fna"), k, 10000000)

  def library(k: Int)(implicit spark: SparkSession) =
    GenomeLibrary(inputs(k), "testData/slacken/seqid2taxid.map")

  def index(k: Int, m: Int, location: Option[String])(implicit spark: SparkSession): KeyValueIndex = {
      val mp = RandomXOR(m, DEFAULT_TOGGLE_MASK, true)

      val splitter = MinSplitter(mp, k)
      val params = IndexParams(spark.sparkContext.broadcast(splitter), 16, location.getOrElse(null))
      new KeyValueIndex(params, TestData.taxonomy)
  }

  def defaultBuckets(idx: KeyValueIndex, k: Int)(implicit spark: SparkSession): DataFrame =
    idx.makeBuckets(TestData.library(k), addRC = false)


}
