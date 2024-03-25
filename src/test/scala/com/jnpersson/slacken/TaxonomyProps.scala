/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.{ROOT, Root}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TaxonomyProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import Testing._
  test("ancestor has correct depth") {
    forAll(taxonomies(100)){ tax =>
      for { t <- tax.taxa
            p = tax.parents(t)
            if t != ROOT
            } {
        val r1 = tax.ranks(t)
        val r2 = tax.ranks(p)
        //NB this property won't hold if the taxonomy is populated with incorrect data to begin with
        r1.depth should be >= r2.depth
        tax.hasAncestor(t, p) should be(true)
      }
    }
  }

  test("children is correctly populated") {
    forAll(taxonomies(100)){ tax =>
      for { t <- tax.taxa
            c <- tax.children(t)
            } {
        tax.parents(c) should equal(t)
      }
    }
  }

  test("isLeafNode") {
    forAll(taxonomies(100)) { tax =>
      for {t <- tax.taxa
           if tax.isLeafNode(t)
           } {
        tax.children(t) should be(empty)
      }
    }
  }

  test("all nodes have paths to root (is DAG)") {
    forAll(taxonomies(100)) { tax =>
      for {t <- tax.taxa
           } {
        tax.stepsToAncestor(t, ROOT) should not equal(-1)
      }
    }
  }

  test("ancestor at level") {
    forAll(taxonomies(100)) { tax =>
      for {t <- tax.taxa
           tr = tax.ranks(t)
           r <- Taxonomy.rankValues
           anc = tax.ancestorAtLevel(t, r)
           } {
        if (r == tr) {
          anc should equal(t)
        } else if (r.isBelow(tr)) {
          //rank is too low, same value should be returned
          anc should equal(t)
        } else if (!r.isBelow(tax.ranks(anc))) { //there might not be an ancestor at the requested level r
          tax.ranks(anc) should equal(r)
        }
      }
    }
  }

  test("LCA") {
    forAll(taxonomies(100)) { tax =>
      val finder = new LowestCommonAncestor(tax)
      for {t <- tax.taxa
           u <- tax.taxa
           } {
        val lca = finder(t, u)
        if (t == u) {
          lca should equal(t)
        } else {
          tax.hasAncestor(t, lca) should be(true)
          tax.hasAncestor(u, lca) should be(true)
        }
      }
    }
  }
}
