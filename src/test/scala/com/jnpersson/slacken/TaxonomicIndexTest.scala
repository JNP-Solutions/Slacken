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
  private def simulateReads(genomes: Seq[(SeqTitle, NTSeq)], length: Int, id: Int,
                            acc: List[InputFragment] = Nil): List[InputFragment] = {
    if (id == 0) {
      acc
    } else {
      val genome = Random.nextInt(genomes.size)
      val read = simulatedRead(genomes(genome)._2, length)
      val taxon = (genome + 2)
      val title = s"$id:$taxon"
      val f = InputFragment(title, 0, read, None)
      simulateReads(genomes, length, id - 1, f :: acc)
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

          val cpar = ClassifyParams(2, true)
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

  test("Supermer method") {
    testIndexType((params, taxonomy) => new SupermerIndex(params, taxonomy), 31)
  }

  test("KeyValue method") {
    testIndexType((params, taxonomy) => new KeyValueIndex(params, taxonomy), 63)
  }
}
