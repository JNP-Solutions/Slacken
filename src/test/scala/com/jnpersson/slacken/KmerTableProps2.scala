/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.TestGenerators.ks
import com.jnpersson.discount.util.KmerTable
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.jnpersson.discount.util.{Testing => UTesting}
import com.jnpersson.discount.util.Testing._
import org.scalatest.matchers.should.Matchers._

class KmerTableProps2 extends AnyFunSuite with ScalaCheckPropertyChecks {
  implicit class PropsEnhancedTable(t: KmerTable) {
    //to help equality checking - arrays don't have deep equals
    def listIterator: Iterator[List[Long]] =
      t.iterator.map(_.toList)

    //as above but for k-mers with nonzero tags
    def presentListIterator: Iterator[List[Long]] = {
      val tag = t.kmerWidth + 1
      t.indexIterator.filter(i => t.kmers(tag)(i) > 0).map(i => t(i).toList)
    }
  }

  test("left join") {
    import UTesting.shrinkEncodedKmerList
    forAll(ks, tagWidths) { (k, tw) =>
      whenever(1 <= k) {
        forAll(Gen.listOfN(5, encodedKmers(k))) { commonPart =>
          val common = commonPart.toArray
          forAll(kmerTable(k, tw, 10, common), kmerTable(k, 2, 10, common)) { (t1, countingTable) =>
            val default = 7L
            val joint = KmerTableOps.leftJoinTables(t1, countingTable, 1, default)

            //Since tables are randomly generated, the true intersection may be larger than "common"
            val s1 = t1.listIterator.toSet
            val s2 = countingTable.listIterator.toSet
            val trueIntersection = s1.intersect(s2)

            val jointItems = joint.listIterator.toList
            val expectedJointNonzeroItems = trueIntersection.toList.
              filter(km => tagsForKmer(countingTable, km.toArray).get(1) != 0).
              distinct
            //TODO: also test case with joint zero count items

            //Check that all joint k-mers exist and have the expected tags
            //NB this test is not yet sophisticated enough to check multiple occurrences of k-mers.
            jointItems should contain allElementsOf expectedJointNonzeroItems
            for {
              kmer <- trueIntersection
              kma = kmer.toArray
            } {
              val expTags = tagsForKmer(t1, kma).get ++ tagsForKmer(countingTable, kma).get
              expectTagsForKmer(joint, kma, expTags)
            }

            //Check that all left only items appear and have the expected default tags
            jointItems should contain allElementsOf t1.listIterator.toList
            val leftOnly = t1.listIterator.toSet -- jointItems
            for {
              kmer <- leftOnly
              kma = kmer.toArray
            } {
              val expTags = tagsForKmer(t1, kma).get ++ Array(0L, default)
              expectTagsForKmer(joint, kma, expTags)
            }

            //Check that all items other than the common items have the expected default tags
            for {
              kmer <- joint
              if ! trueIntersection.contains(kmer.toList)
            } {
              val expTags = tagsForKmer(t1, kmer).get ++ Array(0L, default)
              expectTagsForKmer(joint, kmer, expTags)
            }
          }
        }
      }
    }
  }

}
