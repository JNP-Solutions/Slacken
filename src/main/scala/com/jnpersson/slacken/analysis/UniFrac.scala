package com.jnpersson.slacken.analysis

import com.jnpersson.slacken.Taxonomy
import com.jnpersson.slacken.Taxonomy.ROOT

import scala.collection.mutable.BitSet

class UniFrac(tax: Taxonomy, sample1: BitSet, sample2: BitSet) {

  val tree1: BitSet = fullTree(sample1)
  val tree2: BitSet = fullTree(sample2)
  val sharedTree: BitSet = tree1.intersect(tree2)
  val distinct1: BitSet = tree1 -- sharedTree
  val distinct2: BitSet = tree2 -- sharedTree
  val bothTree = fullTree(sample1 ++ sample2)

  /** Compute the full phylogenetic tree to the root starting from the given leaf nodes */
  def fullTree(sample: BitSet): BitSet = {
    tax.taxaWithAncestors(sample) - ROOT
  }

  def distance: Double = {
    //each distinct node contributes 1 to the unique path length of each sample
    val totalUniquePathLength = distinct1.size + distinct2.size

    //each node contributes 1 to the total path length.
    val totalPathLength = bothTree.size

    totalUniquePathLength.toDouble/totalPathLength
  }

}
