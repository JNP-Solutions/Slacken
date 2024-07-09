/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.spark.SparkSessionTestWrapper
import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class BrackenWeightsTest extends AnyFunSuite with SparkSessionTestWrapper with Matchers {
  implicit val sp = spark
  import spark.sqlContext.implicits._

  test("All reads from the underlying genomes are counted") {
    val k = 35
    val m = 31
    val readLen = 100
    val idx = TestData.index(k, m, None)
    val buckets = TestData.defaultBuckets(idx, k)

    val bw = new BrackenWeights(buckets, idx, readLen)
    val sourceDestCounts = bw.buildWeights(TestData.library(readLen), mutable.BitSet.empty ++ TestData.taxonomy.taxa)
    try {
      val bySource = sourceDestCounts.groupBy("source").
        agg(sum("count").as("totalReads")).as[(Taxon, Long)].collect().toList

      bySource should contain theSameElementsAs TestData.numberOfLength100Reads
    } finally {
      sourceDestCounts.unpersist()
    }
  }
}
