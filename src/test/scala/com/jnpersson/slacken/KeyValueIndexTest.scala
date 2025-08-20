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

import com.jnpersson.kmers.TestGenerators._
import com.jnpersson.kmers.SparkSessionTestWrapper
import com.jnpersson.slacken.Taxonomy.{NONE}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.count
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KeyValueIndexTest extends AnyFunSuite with ScalaCheckPropertyChecks with SparkSessionTestWrapper with Matchers {

  implicit val sp: SparkSession = spark
  import spark.sqlContext.implicits._

  test("Insert testData genomes, write to disk, and check index contents") {
    val location = "testData/slacken/slacken_test_kv"
    val k = 35
    val m = 31
    val s = 7
    val idx = TestData.indexWithRecords(k, m, s, Some(location))
    idx.writeRecords(location)
    val recordCount = KeyValueIndex.load(location, TestData.taxonomy).
      loadRecords(location).filter($"taxon" =!= NONE).count()

    val bcSplit = spark.sparkContext.broadcast(TestData.splitter(k, m, s))
    val distinctMinimizers = TestData.inputs(k).getInputFragments(false).flatMap(f =>
      bcSplit.value.superkmerPositions(f.nucleotides).map(_.rank).toList
    ).distinct().count()

    recordCount should equal(distinctMinimizers)

//    idx.report(None, "slacken_test_stats", None)
  }

  test("Insert random genomes and check index contents") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(dnaStrings(k, 1000)) { x =>
        whenever(k <= x.length) {
          val s = m/3
          val idx = TestData.index(k, m, s, None)
          val taxaSequence = List((1, x)).toDS()
          val recs = idx.makeRecords(taxaSequence)
          val recordCount = recs.groupBy("taxon").agg(count("*")).as[(Taxon, Long)].collect()

          val minCount = idx.split.superkmerPositions(x).map(_.rank).toSeq.toDS().distinct().count()
          List((1, minCount)) should equal(recordCount)
        }
      }
    }
  }

  test("Get spans") {
    val k = 35
    val m = 31
    val s = 7
    val idx = TestData.index(k, m, s, None)
    val fragments = TestData.inputs(k).getInputFragments(withAmbiguous = true).
      map(f => f.copy(nucleotides = f.nucleotides.replaceAll("\\s+", "")))

    val spans = idx.getSpans(fragments, withTitle = true)
    val kmers = spans.map(x => (x.seqTitle.split("\\|")(1).toInt, x.kmers, x.flag)).toDF("title", "kmers", "flag").
      filter($"flag" === SEQUENCE_FLAG).
      groupBy("title").agg(functions.sum("kmers")).
      as[(Taxon, Long)].collect()

    kmers should contain theSameElementsAs TestData.numberOf35Mers
  }

  test("totalKmerCountReport") {
    val k = 31
    val m = 10
    val idx = TestData.indexWithRecords(k, m, 0, None)
    val irs = new IndexStatistics(idx)

    val genomeSizes = irs.totalKmerCountReport(TestData.library(idx.params.k)).genomeSizes.toMap

    val realGenomeSizes = TestData.numberOf31Mers
    genomeSizes should equal(realGenomeSizes)
  }

}
