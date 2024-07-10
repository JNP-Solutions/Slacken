/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.kmers.TestGenerators._

import com.jnpersson.kmers.minimizer._
import com.jnpersson.discount.spark.SparkSessionTestWrapper

import com.jnpersson.kmers.{NTSeq, Testing => DTesting}
import com.jnpersson.kmers.IndexParams
import com.jnpersson.slacken.Taxonomy.{ROOT, Species}
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
  val seqIdToTaxId = leafNodes.map(i => (s"Taxon$i", i))

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
    val genomesDS = genomes.toDF("nucleotides", "header")
    val labels = seqIdToTaxId.toDF("header", "taxon").cache()
    val noiseReads = DTesting.getList(randomReads(200, 200), 1000).
      zipWithIndex.map(x => x._1.copy(header = x._2.toString)).toDS()

    //As this test is slow, we limit the number of checks
    forAll((Gen.choose(15, maxM + 30), "k"), (ms(maxM), "m"), minSuccessful(5)) { (k, m) =>
      whenever (15 <= m && m <= k) {
        forAll(minimizerPriorities(m), minSuccessful(2)) { mp =>
          println(mp)
          val splitter = MinSplitter(mp, k)
          val params = IndexParams(spark.sparkContext.broadcast(splitter), 16, "")
          val idx = makeIdx(params, taxonomy)

          val joint = genomesDS.join(labels, "header").select("taxon", "nucleotides").as[(Taxon, NTSeq)]
          val minimizers = idx.makeBuckets(joint)

          val cpar = ClassifyParams(2, true)
          //The property of known reads classifying correctly.
          val subjectsHits = idx.classify(minimizers, reads)
          idx.classifyHits(subjectsHits, cpar, 0.0).filter(hit => {
            //Check that each read got classified to the expected taxon. In the generated reads
            //the title contains the taxon, as a bookkeeping trick.
            val expTaxon = hit.title.split(":")(1).toInt
            !(!hit.classified || expTaxon == hit.taxon)
          }
          ).isEmpty should be(true)

          //the property of noise reads not classifying. Hard to check with random data for
          //small m. In the future we could generate better test data to get around this.
          if (m >= 30) {
            val subjectsHits = idx.classify(minimizers, noiseReads)
            idx.classifyHits(subjectsHits, cpar, 0.0).filter(r =>
              r.classified
            ).isEmpty should be(true)
          }
        }
      }
    }
  }

  test("Random genomes, KeyValue method") {
    randomGenomesTest((params, taxonomy) => new KeyValueIndex(params, taxonomy), 128)
  }

  test("Tiny index, KeyValue method") {
    val dir = System.getProperty("user.dir")
    val location = s"$dir/testData/slacken/slacken_test_kv"
    val k = 35
    val m = 31
    val idx = TestData.index(k, m, Some(location))
    idx.writeBuckets(TestData.defaultBuckets(idx, k), location)
    KeyValueIndex.load(location, TestData.taxonomy).
      loadBuckets(location).count() should be > 0L
  }

  //Testing the basic code path for dynamic classification.
  //The results aren't yet checked for correctness.
  test("Dynamic index") {
    val k = 35
    val m = 31
    val idx = TestData.index(k, m, None)
    val cpar = ClassifyParams(2, true)

    val dyn = new Dynamic(idx, TestData.library(k),
      Species, 0.1, 100, cpar,
      None, None, false, "")

    val reads = simulateReads(200, 1000).toDS()
    val (buckets, taxa) = dyn.makeBuckets(reads, None)
    val hits = idx.classify(buckets, reads)
  }

  test("A known leaf node total k-mer count is correct with respect to the known value") {

    val k = 31
    val m = 10
    val idx = TestData.index(k, m, None)
    val buckets = TestData.defaultBuckets(idx, k)

    val genomeSizes = idx.totalKmerCountReport(buckets, TestData.library(idx.k)).genomeSizes.toMap
    // The total k-mer counts hardcoded below were independently computed using both KMC3 and Discount
    val realGenomeSizes = Map(526997 -> 2914769, 455631 -> 3594763)
    genomeSizes should equal(realGenomeSizes)
  }
}
