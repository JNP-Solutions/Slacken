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

import com.jnpersson.kmers.TestGenerators.{dnaStrings, minimizerPriorities, ms}
import com.jnpersson.kmers.{IndexParams, MinimizerCLIConf, MinimizerFormats, NTSeq, SparkSessionTestWrapper, Testing => DTesting}
import com.jnpersson.kmers.minimizer.{InputFragment, MinSplitter}
import com.jnpersson.slacken.Taxonomy.Species
import org.apache.spark.sql.SparkSession
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.annotation.tailrec
import scala.util.Random

class ClassifierTest extends AnyFunSuite with ScalaCheckPropertyChecks with SparkSessionTestWrapper with Matchers {

  implicit val sp: SparkSession = spark
  import spark.sqlContext.implicits._

  implicit val formats: MinimizerFormats[MinimizerCLIConf] = SlackenMinimizerFormats

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
  def randomGenomesTest[R](makeIdx: (IndexParams, Taxonomy) => KeyValueIndex, maxM: Int): Unit = {
    //Simulate very long reads to reduce the risk of misclassification by chance in random data
    val reads = simulateReads(200, 1000).toDS()
    val genomesDS = genomes.toDF("nucleotides", "header")
    val labels = seqIdToTaxId.toDF("header", "taxon").cache()
    val noiseReads = DTesting.getList(randomReads(200, 200), 1000).
      zipWithIndex.map(x => x._1.copy(header = x._2.toString)).toDS()

    val location = "testData/slacken/slacken_test_random"

    //As this test is slow, we limit the number of checks
    forAll((Gen.choose(15, maxM + 30), "k"), (ms(maxM), "m"), minSuccessful(5)) { (k, m) =>
      whenever (15 <= m && m <= k) {
        forAll(minimizerPriorities(m), minSuccessful(2)) { mp =>
          println(mp)
          val splitter = MinSplitter(mp, k)
          val params = IndexParams(spark.sparkContext.broadcast(splitter), 16, location)
          val idx = makeIdx(params, taxonomy)

          val joint = genomesDS.join(labels, "header").select("taxon", "nucleotides").as[(Taxon, NTSeq)]
          val minimizers = idx.makeRecords(joint)

          val cpar = ClassifyParams(2, withUnclassified = true)
          //The property of known reads classifying correctly.
          val cls = new Classifier(idx.withRecords(minimizers))
          val subjectsHits = cls.collectHitsBySequence(reads)

          cls.classifyHits(subjectsHits, cpar, 0.0).filter(hit => {
            //Check that each read got classified to the expected taxon. In the generated reads
            //the title contains the taxon, as a bookkeeping trick.
            val expTaxon = hit.title.split(":")(1).toInt
            !(!hit.classified || expTaxon == hit.taxon)
          }
          ).isEmpty should be(true)

          //the property of noise reads not classifying. Hard to check with random data for
          //small m. In the future we could generate better test data to get around this.
          if (m >= 30) {
            val subjectsHits = cls.collectHitsBySequence(noiseReads)
            cls.classifyHits(subjectsHits, cpar, 0.0).filter(r =>
              r.classified
            ).isEmpty should be(true)
          }
        }
      }
    }
  }

  test("Classify with random genomes, KeyValue method") {
    randomGenomesTest((params, taxonomy) => new KeyValueIndex(spark.emptyDataFrame, params, taxonomy), 128)
  }

  test("Direct input classification") {
    val location = "testData/slacken/slacken_test_kv"
    val k = 35
    val m = 31
    val s = 7
    val idx = TestData.indexWithRecords(k, m, s, Some(location))

    //Currently, this is testing the code path without validating the result
    val reads = simulateReads(100, 1000).toDF()
    val slk = new Slacken(idx, false, None, 0, 2, true)
    val classified = slk.classifyReads(reads)
    slk.writeReports(classified, "testData/slacken/di_out")
  }

  test("Dynamic index") {
    val k = 35
    val m = 31
    val s = 7
    val idx = TestData.indexWithRecords(k, m, s, None)
    val cpar = ClassifyParams(2, true)

    val criteria = List(MinimizerTotalCount(100), MinimizerDistinctCount(100),
      ClassifiedReadCount(10, 0))

    val reads = simulateReads(200, 1000).toDS().cache()
    for { c <- criteria } {
      println(s"Testing: $c")
      val dyn = new Dynamic(idx, TestData.library(k),
        Species, c, cpar,
        None, "")

      //Testing the basic code path for dynamic classification.
      //The results aren't yet checked for correctness.
      val (records, taxa) = dyn.makeRecords(reads, None)
      val cls = new Classifier(idx.withRecords(records))
      val hits = cls.collectHitsBySequence(reads)
    }
    reads.unpersist()
  }
}
