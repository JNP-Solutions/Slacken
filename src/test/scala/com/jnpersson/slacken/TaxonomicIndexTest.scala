/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.discount.hash.{InputFragment, MinSplitter}
import com.jnpersson.discount.spark.{IndexParams, SparkSessionTestWrapper}
import com.jnpersson.discount.{Testing => DTesting}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.jnpersson.discount.TestGenerators._
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers

import scala.annotation.tailrec
import scala.util.Random

class TaxonomicIndexTest extends AnyFunSuite with ScalaCheckPropertyChecks with SparkSessionTestWrapper with Matchers {

  implicit val sp = spark
  import spark.sqlContext.implicits._

  val numberOfGenomes = 100
  val taxonomy = TestTaxonomy.make(numberOfGenomes)
  val genomes = com.jnpersson.discount.Testing.getList(TestTaxonomy.genomes, numberOfGenomes - 2).
    zipWithIndex.
    map(g => (s"Taxon${g._2 + 2}", g._1))
  val seqIdToTaxId = (2 until numberOfGenomes).map(i => (s"Taxon$i", i))

  /** Simulate a random read from the given genome */
  def simulatedRead(genome: NTSeq, length: Int = 101): NTSeq = {
    val start = Random.nextInt(genome.length - length)
    genome.substring(start, start + length)
  }

  @tailrec
  private def simulateReads(genomes: Seq[(SeqTitle, NTSeq)], length: Int = 101, n: Int,
                    acc: List[InputFragment] = Nil): List[InputFragment] = {
    if (n == 0) {
      acc
    } else {
      val g = Random.nextInt(genomes.size)
      val r = simulatedRead(genomes(g)._2, length)
      val taxon = (g + 2)
      val title = s"$n:$taxon"
      val f = InputFragment(title, 0, r, None)
      simulateReads(genomes, length, n - 1, f :: acc)
    }
  }

  /* Note: this test is probabilistic and relies on randomly generated DNA sequences
     not having enough k-mers in common.
   */
  def testIndexType[R](makeIdx: (IndexParams, Taxonomy) => TaxonomicIndex[R],
                       maxM: Int): Unit = {
    //Simulate very long reads to reduce the risk of misclassification by chance in random data
    val reads = simulateReads(genomes, 200, 1000).toDS()
    val genomesDS = genomes.toDS
    val labels = seqIdToTaxId.toDS.cache()
    val noiseReads = DTesting.getList(TestTaxonomy.reads(200, 200), 1000).
      zipWithIndex.map(x => x._1.copy(header = x._2.toString)).toDS()

    //As this test is slow, we limit the number of checks
    forAll((Gen.choose(15, 91), "k"), (ms(maxM), "m"), minSuccessful(5)) { (k, m) =>
      whenever (15 <= m && m <= k) {
        forAll(Testing.minimizerPriorities(m), minSuccessful(2)) { mp =>
          println(mp)
          val splitter = MinSplitter(mp, k)
          val params = IndexParams(spark.sparkContext.broadcast(splitter), 16, "")
          val idx = makeIdx(params, taxonomy)

          val minimizers = idx.makeBuckets(genomesDS, labels)

          //The property of known reads classifying correctly.
          idx.classify(minimizers, reads, 2).filter(r => {
            //Check that each read got classified to the expected taxon. In the generated reads
            //the title contains the taxon, as a bookkeeping trick.
            val expTaxon = r.title.split(":")(1).toInt
            !(r.classified || expTaxon != r.taxon)
          }
          ).isEmpty should be(true)

          //the property of noise reads not classifying. Hard to check with random data for
          //small m. In the future we could generate better test data to get around this.
          if (m >= 25) {
            idx.classify(minimizers, noiseReads, 2).filter(r =>
              r.classified
            ).isEmpty should be(true)
          }
        }
      }
    }
  }

  test("Kraken 1 method") {
    testIndexType((p, t) => new SupermerIndex(p, t), 31)
  }

  test("Kraken 2 method") {
    testIndexType((p, t) => new KeyValueIndex(p, t), 63)
  }
}
