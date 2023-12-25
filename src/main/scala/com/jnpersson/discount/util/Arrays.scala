/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.discount.util

import scala.reflect.ClassTag

object Arrays {

  /** Populate a new array with a repeated value, without boxing for primitives.
   * @param size The size of the array
   * @param elem The value
   * */
  def fillNew[@specialized T : ClassTag](size: Int, elem: T): Array[T] = {
    val r = new Array[T](size)
    var i = 0
    while (i < size) {
      r(i) = elem
      i += 1
    }
    r
  }

  /** Sum an int array without boxing. */
  def sum(ints: Array[Int]): Long = {
    var r = 0
    var i = 0
    while (i < ints.length) {
      r += ints(i)
      i += 1
    }
    r
  }

  /** Find the max value without boxing. */
  def max(ints: Array[Int]): Int = {
    var r = Int.MinValue
    var i = 0
    while (i < ints.length) {
      if (ints(i) > r) r = ints(i)
      i += 1
    }
    r
  }
}
