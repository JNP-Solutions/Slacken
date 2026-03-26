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

import com.jnpersson.slacken.Taxonomy.ROOT
import org.scalacheck.{Gen, Prop, Properties}
import org.scalacheck.Prop._

class TaxonomyProps extends Properties("Taxonomy") {
  import Testing._

  def all(ps: TraversableOnce[Prop]): Prop =
    if (ps.isEmpty) proved
    else Prop(prms => ps.map(p => p(prms)).reduceLeft(_ && _))

  property("ancestor has correct depth") =
    forAll(taxonomies(100)) { tax =>
      all(
        tax.taxa.filter(_ != ROOT).map { t =>
          val p = tax.parents(t)
          val r1 = tax.ranks(t)
          val r2 = tax.ranks(p)
          //NB this property won't hold if the taxonomy is populated with incorrect data to begin with
          (r1.depth >= r2.depth) :| s"rank depth of $t >= parent $p" &&
            tax.hasAncestor(t, p) :| s"$t has ancestor $p"
         }
      )
    }

  property("children is correctly populated") =
    forAll(taxonomies(100)) { tax =>
      all(
        tax.taxa.flatMap { t =>
          tax.children(t).map { c =>
            (tax.parents(c) == t) :| s"parent of child $c should be $t"
          }
        }
      )
    }

  property("isLeafNode") =
    forAll(taxonomies(100)) { tax =>
      all(
        tax.taxa.filter(tax.isLeafNode).map { t =>
          tax.children(t).isEmpty :| s"leaf node $t should have no children"
        }
      )
    }

  property("all nodes have paths to root (is DAG)") =
    forAll(taxonomies(100)) { tax =>
      all(
        tax.taxa.map { t =>
          val steps = tax.stepsToAncestor(t, ROOT)
          (steps != -1) :| s"$t has a path to root" &&
            (tax.pathToRoot(t).size == steps + 1) :| s"pathToRoot size matches stepsToAncestor for $t"
        }
      )
    }

  property("ancestor at level") =
    forAll(taxonomies(100)) { tax =>
      all(
        tax.taxa.flatMap { t =>
          val tr = tax.ranks(t)
          Taxonomy.rankValues.map { r =>
            val anc = tax.ancestorAtLevel(t, r)
            if (r == tr) {
              (anc == Some(t)) :| s"ancestorAtLevel($t, $r) should be Some($t)"
            } else if (r < tr) {
              val firstAncestor = tax.pathToRoot(t).find(tax.depth(_) == r.depth)
              (firstAncestor == anc) :| s"ancestorAtLevel($t, $r) should match first ancestor at depth"
            } else {
              (anc == None) :| s"ancestorAtLevel($t, $r) should be None for too-low rank"
            }
          }
        }
      )
    }

  property("standard ancestor at level") =
    forAll(taxonomies(100)) { tax =>
      all(
        tax.taxa.flatMap { t =>
          val tr = tax.ranks(t)
          Taxonomy.rankValues.map { r =>
            val anc = tax.standardAncestorAtLevel(t, r)
            if (r <= tr) {
              val lastAncestor = tax.pathToRoot(t).filter(tax.depth(_) >= r.depth).toSeq.lastOption
              (anc == lastAncestor) :| s"standardAncestorAtLevel($t, $r) should match last ancestor at depth"
            } else {
              (anc == None) :| s"standardAncestorAtLevel($t, $r) should be None for too-low rank"
            }
          }
        }
      )
    }

  property("LCA") =
    forAll(taxonomies(100)) { tax =>
      val finder = new LowestCommonAncestor(tax)
      all(
        tax.taxa.flatMap { t =>
          tax.taxa.map { u =>
            val lca = finder(t, u)
            if (t == u)
              (lca == t) :| s"LCA($t, $t) should be $t"
            else
              tax.hasAncestor(t, lca) :| s"$t has ancestor LCA=$lca" &&
                tax.hasAncestor(u, lca) :| s"$u has ancestor LCA=$lca"
          }
        }
      )
    }

  property("taxaWithDescendants") =
    forAll(taxonomies(100)) { tax =>
      forAll(Gen.someOf(tax.taxa.toList)) { subset =>
        val withDescendants = tax.taxaWithDescendants(subset)
        val subsetSet = subset.toSet

        subsetSet.subsetOf(withDescendants) :| "all taxa in subset are in result" &&
          (withDescendants -- subset).forall(t =>
            subset.exists(a => tax.hasAncestor(t, a))) :| "every added descendant has an ancestor in subset" &&
          ((withDescendants ++ subset.iterator.flatMap(t => tax.children(t))) == withDescendants) :| "all children already added"
      }
    }

  property("taxaWithAncestors") =
    forAll(taxonomies(100)) { tax =>
      forAll(Gen.someOf(tax.taxa.toList)) { subset =>
        val withAncestors = tax.taxaWithAncestors(subset)
        val subsetSet = subset.toSet
        val allAncestors = subset.iterator.flatMap(t => tax.pathToRoot(t)).toSet

        subsetSet.subsetOf(withAncestors) :| "all taxa in subset are in result" &&
          (allAncestors == withAncestors) :| "result equals all ancestors of subset"
      }
    }
}
