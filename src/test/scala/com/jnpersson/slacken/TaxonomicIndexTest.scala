/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount
import com.jnpersson.discount.TestGenerators._
import com.jnpersson.discount.hash.{DEFAULT_TOGGLE_MASK, ExtendedTable, InputFragment, MinSplitter, MinimizerPriorities, RandomXOR}
import com.jnpersson.discount.spark.{Discount, IndexParams, SparkSessionTestWrapper}
import com.jnpersson.discount.{NTSeq, Testing => DTesting}
import com.jnpersson.slacken.Taxonomy.ROOT
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.annotation.tailrec
import scala.util.Random

class TaxonomicIndexTest extends AnyFunSuite with ScalaCheckPropertyChecks with SparkSessionTestWrapper with Matchers {

  implicit val sp = spark
  import spark.sqlContext.implicits._

  val numberOfGenomes = 100
  //multiply by 8 to make the leaf node level approximately right sized
  val taxonomy = DTesting.getSingle(Testing.taxonomies(numberOfGenomes * 8))

  val leafNodes = taxonomy.taxa.filter(taxonomy.isLeafNode).toList
  val genomes = DTesting.getList(dnaStrings(1000, 10000), leafNodes.size).
    zip(leafNodes).
    map(g => (s"Taxon${g._2 + 2}", g._1))
  val seqIdToTaxId = (leafNodes).map(i => (s"Taxon$i", i))

  /** Simulate a random read from the given genome */
  def simulatedRead(genome: NTSeq, length: Int = 101): NTSeq = {
    val start = Random.nextInt(genome.length - length)
    genome.substring(start, start + length)
  }

  @tailrec
  private def simulateReads(length: Int, id: Int, acc: List[InputFragment] = Nil): List[InputFragment] = {
    if (id == 0) {
      acc
    } else {
      val genome = Random.nextInt(genomes.size)
      val read = simulatedRead(genomes(genome)._2, length)
      val taxon = genome + 2
      val title = s"$id:$taxon"
      val f = InputFragment(title, 0, read, None)
      simulateReads(length, id - 1, f :: acc)
    }
  }

  def randomReads(minLen: Int, maxLen: Int): Gen[InputFragment] =
    dnaStrings(minLen, maxLen).map(ntseq => InputFragment("", 0, ntseq, None))

  /* Note: this test is probabilistic and relies on randomly generated DNA sequences
     not having enough k-mers in common.
   */
  def randomGenomesTest[R](makeIdx: (IndexParams, Taxonomy) => TaxonomicIndex[R], maxM: Int): Unit = {
    //Simulate very long reads to reduce the risk of misclassification by chance in random data
    val reads = simulateReads(200, 1000).toDS()
    val genomesDS = genomes.toDS
    val labels = seqIdToTaxId.toDS.cache()
    val noiseReads = DTesting.getList(randomReads(200, 200), 1000).
      zipWithIndex.map(x => x._1.copy(header = x._2.toString)).toDS()

    //As this test is slow, we limit the number of checks
    forAll((Gen.choose(15, 91), "k"), (ms(maxM), "m"), minSuccessful(5)) { (k, m) =>
      whenever (15 <= m && m <= k) {
        forAll(minimizerPriorities(m), minSuccessful(2)) { mp =>
          println(mp)
          val splitter = MinSplitter(mp, k)
          val params = IndexParams(spark.sparkContext.broadcast(splitter), 16, "")
          val idx = makeIdx(params, taxonomy)

          val minimizers = idx.makeBuckets(genomesDS, labels, LCAAtLeastTwo)

          val cpar = ClassifyParams(2, 0, true)
          //The property of known reads classifying correctly.
          idx.classify(minimizers, reads, cpar).filter(r => {
            //Check that each read got classified to the expected taxon. In the generated reads
            //the title contains the taxon, as a bookkeeping trick.
            val expTaxon = r.title.split(":")(1).toInt
            !(r.classified || expTaxon != r.taxon)
          }
          ).isEmpty should be(true)

          //the property of noise reads not classifying. Hard to check with random data for
          //small m. In the future we could generate better test data to get around this.
          if (m >= 25) {
            idx.classify(minimizers, noiseReads, cpar).filter(r =>
              r.classified
            ).isEmpty should be(true)
          }
        }
      }
    }
  }

  test("Random genomes, supermer method") {
    randomGenomesTest((params, taxonomy) => new SupermerIndex(params, taxonomy), 31)
  }

  test("Random genomes, KeyValue method") {
    randomGenomesTest((params, taxonomy) => new KeyValueIndex(params, taxonomy), 63)
  }

  /**
   * A hardcoded taxonomy for the tiny test dataset in testData/slacken/slacken_tinydata.fna.
   * Make both strains direct children of root as a simple way to generate test data.
   */
  def testDataTaxonomy =
    Taxonomy.fromNodesAndNames(
      Array((455631, ROOT, "strain"),
        (526997, ROOT, "strain")),
      Iterator((455631, "Clostridioides difficile QCD-66c26"),
        (526997, "Bacillus mycoides DSM 2048"))
    )

  /* Build a tiny index and write it to disk, then reading it back.
  * At this point there's no correctness check, but the code path is tested.
  */
  def makeTinyIndex[R](makeIdx: (IndexParams, Taxonomy) => TaxonomicIndex[R],
                       loadIdx: String => TaxonomicIndex[R],
                       location: String): Unit = {
    val k = 31
    val m = 10
    val mp = RandomXOR(m, DEFAULT_TOGGLE_MASK, true)

    val splitter = MinSplitter(mp, k)
    val params = IndexParams(spark.sparkContext.broadcast(splitter), 16, "")
    val idx = makeIdx(params, testDataTaxonomy)
    val discount = Discount(k)
    val bkts = idx.makeBuckets(discount, List("testData/slacken/slacken_tinydata.fna"),
      "testData/slacken/seqid2taxid.map", addRC = false)
    idx.writeBuckets(bkts, location)
    loadIdx(location).loadBuckets(location).count() should be > 0L
  }

  test("Tiny index, supermer method") {
    val dir = System.getProperty("user.dir")
    makeTinyIndex((params, taxonomy) => new SupermerIndex(params, taxonomy),
      location => SupermerIndex.load(location, testDataTaxonomy),
      s"$dir/testData/slacken/slacken_test_sm")
  }

  test("Tiny index, KeyValue method") {
    val dir = System.getProperty("user.dir")
    makeTinyIndex((params, taxonomy) => new KeyValueIndex(params, taxonomy),
      location => KeyValueIndex.load(location, testDataTaxonomy),
      s"$dir/testData/slacken/slacken_test_kv")
  }
}
