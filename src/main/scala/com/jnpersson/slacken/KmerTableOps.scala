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

package com.jnpersson.slacken

import com.jnpersson.discount.bucket.Reducer
import com.jnpersson.discount.util.{KmerTable, KmerTableBuilder}

object KmerTableOps {

  /**
   * Left join k-mers from query table with a counting table (zero-count k-mers will get a default value).
   * Both tables must be sorted. All copies of matching k-mers from this table will be selected.
   * Each resulting item will contain: (k-mer data) (tags from query table) (counting tags).
   * Note: should consider making this aware of the [[Reducer]] logic to generalize beyond counting tables
   *
   * @param counting
   * @param countColumn 0-based offset of the counting tag
   * @return
   */
  def leftJoinTables(query: KmerTable, counting: KmerTable,
                     countColumn: Int, defaultCount: Long): KmerTable = {
    var qIdx = 0
    var cIdx = 0
    if (query.isEmpty) {
      return KmerTable.builder(query.k, 0, query.tagWidth + counting.tagWidth).result(false)
    }

    //Resulting table should have room for k-mer, our tags, query tags
    val r = new KmerTableBuilder(query.width + counting.tagWidth, query.tagWidth + counting.tagWidth, query.size,
      query.k)

    while (qIdx < query.size) {
      val c = if (cIdx < counting.size) {
        query.compareKmers(qIdx, counting, cIdx)
      } else -1

      if (c < 0) {
        //counting is ahead, or we have exhausted counting
        query.copyKmerAndTagsToBuilder(r, qIdx)

        //Insert in result as default value
        var i = 0
        while (i < countColumn) {
          r.addLong(0)
          i += 1
        }
        r.addLong(defaultCount)

        qIdx += 1
      } else if (c > 0) {
        //query is ahead and we have not exhausted counting
        cIdx += 1
      } else {
        if (counting.kmers(counting.kmerWidth + countColumn)(cIdx) != 0) {
          //Found a match, copy it and its associated tag data
          query.copyKmerAndTagsToBuilder(r, qIdx)
          counting.copyTagsOnlyToBuilder(r, cIdx)
          qIdx += 1
        } else {
          //If the count tag was 0, we don't advance the qIdx as the same k-mer may appear again
          //with a different count tag in the counting table
          cIdx += 1
        }
      }
    }

    r.result(false)
  }

}
