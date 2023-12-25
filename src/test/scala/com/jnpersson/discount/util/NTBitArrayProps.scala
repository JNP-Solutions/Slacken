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

import com.jnpersson.discount.TestGenerators._
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class NTBitArrayProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import BitRepresentation._

  test("length") {
    forAll(dnaStrings) { x =>
      NTBitArray.encode(x).size should equal(x.length)
    }
  }

  test("decoding") {
    forAll(dnaStrings) { x =>
      NTBitArray.encode(x).toString should equal(x)
    }
  }

  test("partAsLongArray identity") {
    forAll(dnaStrings) { x =>
      val enc = NTBitArray.encode(x)
      val buf = enc.partAsLongArray(0, enc.size)
      java.util.Arrays.equals(buf, enc.data) should be(true)
    }
  }

  test("sliceAsCopy decoding") {
    forAll(dnaStrings) { x =>
      val enc = NTBitArray.encode(x)
      forAll(Gen.choose(0, x.length), Gen.choose(0, x.length)) { (offset, length) =>
        whenever(offset + length <= x.length) {
          val slice = enc.sliceAsCopy(offset, length)
          slice.toString should equal(x.substring(offset, offset + length))
        }
      }
    }
  }

  test("k-mers length") {
    forAll(dnaStrings, ks) { (x, k) =>
      whenever (k <= x.length) {
        val kmers = KmerTable.fromSegment(NTBitArray.encode(x), k, forwardOnly = false)
        kmers.size should equal (x.length - (k - 1))
      }
    }
  }

  test("k-mers data") {
    forAll(dnaStrings, ks) { (x, k) =>
      whenever (k <= x.length && k >= 1 && x.nonEmpty) {
        val kmers = NTBitArray.encode(x).kmersAsLongArrays(k).toArray
        val dec = NTBitArray.fixedSizeDecoder(k)
        val kmerStrings = kmers.map(km => dec.longsToString(km, 0, k))
        kmerStrings.toList should equal (x.sliding(k).toList)
      }
    }
  }

  test("shift k-mer left") {
    forAll(dnaStrings, ks, dnaLetterTwobits) { (x, k, letter) =>
      whenever (k <= x.length && k >= 1 && x.nonEmpty) {
        val first = NTBitArray.encode(x).partAsLongArray(0, k)
        NTBitArray.shiftLongArrayKmerLeft(first, letter, k)
        val enc2 = NTBitArray.encode(x.substring(1, k) + twobitToChar(letter))
        java.util.Arrays.equals(first, enc2.data) should be(true)
      }
    }
  }

  test("reverse complement") {
    forAll(dnaStrings) { x =>
      whenever (x.nonEmpty) {
        val enc = NTBitArray.encode(x)
        enc.reverseComplement.toString should equal(DNAHelpers.reverseComplement(x))
        enc.reverseComplement.reverseComplement.compareTo(enc) should equal(0)

        val can = enc.canonical
        (can.toString == x || can.toString == DNAHelpers.reverseComplement(x)) should be(true)
        (can > enc) should be(false)
      }
    }
  }

  test("comparison") {
    forAll(ks) { k =>
      whenever(k > 0) {
        forAll(dnaStrings(k, k), dnaStrings(k, k)) { (x, y) =>
          whenever(x.nonEmpty && y.nonEmpty) {
            val e1 = NTBitArray.encode(x)
            val e2 = NTBitArray.encode(y)
            Math.signum(e1.compareTo(e2)) should equal(Math.signum(x.compareTo(y)))
            e1.compareTo(e1) should equal(0)
          }
        }
      }
    }
  }

  test("clone") {
    forAll(dnaStrings) { x =>
      val enc = NTBitArray.encode(x)
      enc.clone().toString should equal(x)
      enc should equal(enc.clone())
    }
  }

  test("left shift") {
    forAll(dnaStrings) { x =>
      val enc = NTBitArray.encode(x)
      forAll(Gen.choose(0, x.size)) { offset =>
        whenever(offset <= x.size && offset >= 0) {
          (enc.clone() <<= 0).toString should equal(x)
          (enc.clone() <<= offset * 2).toString should equal(x.substring(offset) + ("A" * offset))
        }
      }
    }
  }

  test("right shift") {
    forAll(dnaStrings) { x =>
    val enc = NTBitArray.encode(x)
    forAll(Gen.choose(0, x.size)) { offset =>
      whenever(offset <= x.size && offset >= 0) {
        (enc.clone() >>>= 0).toString should equal(x)
        (enc.clone() >>>= offset * 2).toString should equal(("A" * offset) + x.substring(0, x.size - offset))
      }
    }
  }
}

  test("toLong") {
    forAll(dnaStrings(1, 31)) { x =>
      whenever(x.size <= 31) {
        val enc = NTBitArray.encode(x)
        NTBitArray.fromLong(enc.toLong, x.size) should equal(enc)
      }
    }
  }

  test("toInt") {
    forAll(dnaStrings(1, 15)) { x =>
      whenever(x.size <= 15) {
        val enc = NTBitArray.encode(x)
        NTBitArray.fromLong(enc.toInt, x.size) should equal(enc)
      }
    }
  }

}
