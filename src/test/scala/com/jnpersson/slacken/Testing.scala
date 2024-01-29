/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount
import com.jnpersson.discount.NTSeq
import com.jnpersson.discount.TestGenerators.dnaStrings
import com.jnpersson.discount.hash.{ExtendedTable, InputFragment, MinimizerPriorities}
import org.scalacheck.Gen

import scala.util.Random

object Testing {
  def extendedTable(e: Int, m: Int): Gen[ExtendedTable] = {
    val inner = discount.Testing.minTable(m)
    for { canonical <- Gen.oneOf(true, false)
          withSuf <- Gen.oneOf(true, false) }
    yield ExtendedTable(inner, e, canonical, withSuf)
  }

  def minimizerPriorities(m: Int): Gen[MinimizerPriorities] = {
    if (m >= 30) {
      //ExtendedTable only works for somewhat large m
      Gen.oneOf(discount.TestGenerators.minimizerPriorities(m), extendedTable(m, 10))
    } else {
      discount.TestGenerators.minimizerPriorities(m)
    }
  }
}

object TestTaxonomy {

  val genomes: Gen[NTSeq] = dnaStrings(1000, 10000)

  /** Generate a taxonomy that contains nodes 1..nodes - 1 */
  def make(nodes: Int): Taxonomy = {
    //Skip node 0 = NONE and 1 = ROOT
    val ns = (2 until nodes).map(n => {
      //every node n gets a parent 1 <= p < n, which guarantees a DAG structure with 1 as root, no cycles
      val parent = Random.nextInt(n - 1) + 1
      val rank = "S"
      (n, parent, rank)
    })
    val names = (0 until nodes).iterator.map(n => (n, s"Taxon $n"))
    Taxonomy.fromNodesAndNames(ns.toArray, names)
  }

  def reads(minLen: Int, maxLen: Int): Gen[InputFragment] =
    dnaStrings(minLen, maxLen).map(ntseq => InputFragment("", 0, ntseq, None))
}