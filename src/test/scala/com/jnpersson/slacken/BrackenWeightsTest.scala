/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.SparkSessionTestWrapper
import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class BrackenWeightsTest extends AnyFunSuite with SparkSessionTestWrapper with Matchers {
  implicit val sp = spark
  import spark.sqlContext.implicits._

  test("Read counts from the underlying genomes are correct") {
    val k = 35
    val m = 31
    val readLen = 100
    val idx = TestData.index(k, m, None)
    val buckets = TestData.defaultBuckets(idx, k)

    val bw = new BrackenWeights(buckets, idx, readLen)
    val sourceDestCounts = bw.buildWeights(TestData.library(readLen), mutable.BitSet.empty ++ TestData.taxonomy.taxa)
    try {
      sourceDestCounts.as[(Int, Int, Long)].collect() should contain theSameElementsAs(TestData.brackenWeightsLength100)
    } finally {
      sourceDestCounts.unpersist()
    }
  }
}
