/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.slacken.analysis

import com.jnpersson.slacken.Taxonomy
import com.jnpersson.slacken.Taxonomy.Species
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import MappingComparison._

class MappingComparisonTest extends AnyFunSuite with Matchers {
  val tax = {
    val nodes = List((1, 1, "root"),
      (2, 1, "genus"),
      (3, 2, "species"), //S level
      (4, 3, "species"), //S1
      (5, 4, "species"), //S2
      (6, 2, "species"), //S
    )
    val names = nodes.map(x => (x._1, s"Node ${x._1}"))

    Taxonomy.fromNodesAndNames(nodes, names.iterator, Seq.empty)
  }

  test("hit categories for mapping comparison") {
    hitCategory(tax, 2, 2, Some(Species)) should equal(TruePos)

    hitCategory(tax, 3, 2, Some(Species)) should equal(VaguePos(1))
    hitCategory(tax, 4, 2, Some(Species)) should equal(VaguePos(1))
    hitCategory(tax, 3, 1, Some(Species)) should equal(VaguePos(8)) //Root level - species level

    hitCategory(tax, 3, 6, Some(Species)) should equal(FalsePos)

    //Reference is more specific than test but both are on the same standardised species level,
    //so this should not be a vague pos
    hitCategory(tax, 4, 3, Some(Species)) should equal(TruePos)

    hitCategory(tax, 3, Taxonomy.NONE, Some(Species)) should equal(FalseNeg)

    //Test is more specific than reference. Considered to be a special case of true positive.
    hitCategory(tax, 3, 5, Some(Species)) should equal(TruePos)
  }
}
