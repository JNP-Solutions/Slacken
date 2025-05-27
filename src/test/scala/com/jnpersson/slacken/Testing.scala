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

import com.jnpersson.kmers
import com.jnpersson.kmers.input.FileInputs
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.{AnyMinSplitter, IndexParams, TestGenerators, Testing => TTesting}
import com.jnpersson.slacken.Taxonomy.{NONE, ROOT, Rank, Root}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalacheck.{Gen, Shrink}

import scala.annotation.tailrec

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
    Taxonomy(parents, tax.ranks, tax.scientificNames, tax.primary)
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
   * An equal number of nodes at every taxonomic rank will be generated (except Root,
   * which will have one node).
   * @param size Approximate number of nodes in the taxonomy (at least this many is guaranteed)
   */
  def taxonomies(size: Int): Gen[Taxonomy] = {
    import org.scalacheck.util.Buildable.buildableSeq

    // Guarantee at least size number of nodes in the taxonomy
    val levelSize = size / (Taxonomy.rankValues.size - 1) + 1
    //Generate data for every level (rank) in the tree and then compose them
    val levelGenerators: Gen[Seq[(Taxon, Taxon, String)]] =
      Gen.sequence(for {
      rank <- Taxonomy.rankValues
      if rank != Taxonomy.Root
      maxParent = (rank.depth - 1) * levelSize + 1 //each level has a fixed ID range
      id <- ((rank.depth - 1) * levelSize + 2) until (rank.depth * levelSize + 2)
    } yield taxonAtLevel(id, rank, maxParent))

    val rootNode = (1, 1, Root.title)
    levelGenerators.map(nodes => {
//      println(nodes)
      val all = (rootNode +: nodes).toArray
      Taxonomy.fromNodesAndNames(all, all.iterator.map(n => (n._1, s"Taxon ${n._1}")), Seq.empty)
    })
  }

  def minimizerPriorities(m: Int): Gen[MinimizerPriorities] =
      kmers.TestGenerators.minimizerPriorities(m)

  /** Generate taxon hits from a read with taxa randomly selected from the given array and a
   * given number of total k-mers.
   */
  def pseudoRead(taxa: Array[Taxon], totalKmers: Int, maxHitSize: Int): Gen[List[TaxonHit]] =
    for {
      kmersPerHit <- termsToSum(totalKmers, maxHitSize)
      taxa <- Gen.listOfN(kmersPerHit.size, Gen.oneOf(taxa))
    } yield kmersPerHit.zip(taxa).map(kt => TaxonHit(true, 0, kt._2, kt._1))

  def permutations[T](items: Seq[T]): Gen[Vector[T]] =
    permutations(items.toVector)

  def permutations[T](items: Vector[T]): Gen[Vector[T]] = {
    if (items.size < 2) {
      Gen.const(items)
    } else {
      for {
        i <- Gen.choose(0, items.size - 1)
        x = items(i)
        prefix <- permutations(items.take(i))
        suffix <- permutations(items.drop(i + 1))
      } yield x +: (prefix ++ suffix)
    }
  }

  /** Pick a list prefix such that the sum of the terms equals the given value.
   *
   * @param remaining target sum
   * @param terms     terms to pick from
   * @param acc       accumulator
   * @return terms such that the sum of them all equals the initial target sum, in reversed order
   */
  @tailrec
  def prefixToSum(remaining: Int, terms: List[Int], acc: List[Int]): List[Int] = {
    terms match {
      case x :: xs =>
        if (x >= remaining)
          remaining :: acc //Adjust the final term to guarantee that the total sum is correct
        else
          prefixToSum(remaining - x, xs, x :: acc)
      case _ => acc
    }
  }

  /** Generate possible ways to sum up nonzero integer terms to a given total */
  def termsToSum(sum: Int, maxTerm: Int): Gen[List[Int]] = {
    //First, generate at least "sum" number of random integers that are at least 1.
    //The sum of them all is between sum and sum * maxTerm.
    //Then take only the prefix that is long enough to satisfy that the total sum should be correct.
    Gen.listOfN(sum, Gen.choose(1, maxTerm)).map(list => prefixToSum(sum, list, Nil))
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
        (526997, ROOT, "strain"),
        (9606, ROOT, "species")),
      Iterator((455631, "Clostridioides difficile QCD-66c26"),
        (526997, "Bacillus mycoides DSM 2048"),
        (9606, "Homo sapiens")),
      Seq.empty
    )

  //Number of length 100 reads for each taxon (including masked or invalid regions).
  //Manually calculated from the .fai files
  val numberOfLength100Reads = List(
    (455631, 4126265L),
    (526997, 3070413L),
    (9606, 799821)
  )

  //Reference bracken weights
  val brackenWeightsLength100 =
    List[(Int, Int, Long)]((455631, 455631, 3924809), (0, 455631, 201425), (0, 526997, 29747),
    (526997, 526997, 3040666), (1, 455631, 31), (0, 9606, 159860), (9606, 9606, 639961))

  // The total k-mer counts hardcoded below were independently computed using both KMC3 and Discount
  val numberOf31Mers = Map(526997 -> 2914769, 455631 -> 3594763, 9606 -> 639800)
  val numberOf35Mers = Map(526997 -> 2902850, 455631 -> 3565872, 9606 -> 639784)

  def inputs(k: Int)(implicit spark: SparkSession) =
    new FileInputs(List("testData/slacken/slacken_tinydata.fna"), k, 10000000)

  def library(k: Int)(implicit spark: SparkSession): GenomeLibrary =
    GenomeLibrary(inputs(k), "testData/slacken/seqid2taxid.map")

  def minPriorities(m: Int, s: Int): MinimizerPriorities =
    if (s > 0)
      SpacedSeed(s, RandomXOR(m, DEFAULT_TOGGLE_MASK, true))
    else
      RandomXOR(m, DEFAULT_TOGGLE_MASK, true)

  def splitter(k: Int, m: Int, s: Int): AnyMinSplitter =
    MinSplitter(minPriorities(m, s), k)

  def index(k: Int, m: Int, s: Int, location: Option[String])(implicit spark: SparkSession): KeyValueIndex = {
      val params = IndexParams(spark.sparkContext.broadcast(splitter(k, m, s)), 16, location.orNull)
      new KeyValueIndex(spark.emptyDataFrame, params, TestData.taxonomy)
  }

  def defaultRecords(idx: KeyValueIndex, k: Int)(implicit spark: SparkSession): DataFrame =
    idx.makeRecords(TestData.library(k))

  def indexWithRecords(k: Int, m: Int, s: Int, location: Option[String])(implicit spark: SparkSession): KeyValueIndex = {
    val i = index(k, m, s, location)
    i.withRecords(defaultRecords(i, k))
  }
}
