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

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers.{SeqLocation, _}
import com.jnpersson.kmers.util.NTBitArray

/**
 * A sequence fragment with a controlled maximum size. Does not contain whitespace.
 *
 * @param header Title/header of the sequence
 * @param location 1-based location in the source sequence
 * @param nucleotides Nucleotides in the source sequence
 * @param nucleotides2 Nucleotides for the second half in a pair, for paired end reads
 */
final case class InputFragment(header: SeqTitle, location: SeqLocation, nucleotides: NTSeq,
  nucleotides2: Option[NTSeq])

/**
 * A hashed segment (i.e. a superkmer, where every k-mer shares the same minimizer)
 * with minimizer, sequence ID, and 1-based sequence location
 *
 * @param hash        hash (minimizer)
 * @param sequence    Sequence ID/header
 * @param location    Sequence location (1-based) if available
 * @param nucleotides Encoded nucleotides of this segment
 */
final case class SplitSegment(hash: BucketId, sequence: SeqID, location: SeqLocation, nucleotides: NTBitArray) {

  /**
   * Obtain a human-readable (decoded) version of this SplitSegment
   * @param splitter The splitter object that generated this segment
   * @return
   */
  def humanReadable(splitter: AnyMinSplitter): (String, SeqID, SeqLocation, NTSeq) =
    (splitter.humanReadable(hash), sequence, location, nucleotides.toString)

}

/**
 * @param location location of superkmer
 * @param rank encoded priority of minimizer; uniquely identifies it
 * @param length length of superkmer
 */
final case class Minimizer(location: Int, rank: Array[Long], length: Int)

/**
 * @param rank encoded priority of minimizer; uniquely identifies it
 * @param nucleotides encoded super-mer
 * @param location location in sequence if available
 */
final case class Supermer(rank: Array[Long], nucleotides: NTBitArray, location: SeqLocation)

object MinSplitter {
  /** Estimated bin size (sampled count of a minimizer, scaled up) that is considered a "large" bucket
   * This can be used to help determine the best counting method. */
  val largeThreshold = 5000000

  /** Code for invalid minimizers */
  val INVALID: NTBitArray = NTBitArray.empty
}

/**
 * Split reads into superkmers by ranked motifs (minimizers). Such superkmers can be bucketed by the corresponding
 * minimizer.
 * @param priorities Minimizer ordering to use for splitting
 * @param k k-mer length
 */
final case class MinSplitter[+P <: MinimizerPriorities](priorities: P, k: Int) {
  if (priorities.numLargeBuckets > 0) {
    println(s"${priorities.numLargeBuckets} motifs are expected to generate large buckets.")
  }

  /** A ShiftScanner associated with this splitter's MinTable */
  @transient
  lazy val scanner: ShiftScanner = ShiftScanner(priorities)

  /** Split a read into superkmers.
   * @param read the read to split
   * @return an iterator of (rank (hash/minimizer ID), encoded superkmer, location in sequence if available)
   */

  def splitEncode(read: NTSeq): Iterator[Supermer] = {
    val enc = scanner.allMatches(read)
    splitRead(enc._1, enc._2)
  }

  /** Split an encoded read into superkmers.
   * @param encoded the read to split
   * @return an iterator of (rank (hash/minimizer ID), encoded superkmer, position of superkmer start in sequence)
   */
  def splitRead(encoded: NTBitArray, reverseComplement: Boolean = false): Iterator[Supermer] = {
    val enc = scanner.allMatches(encoded, reverseComplement)
    splitRead(enc._1, enc._2)
  }

  /** Split a read into super-mers, returning only the position and length of each.
   * @return an iterator of (position in sequence, minimizer rank, length of superkmer) */
  def superkmerPositions(read: NTSeq): Iterator[Minimizer] = {
    val enc = scanner.allMatches(read)
    superkmerPositions(enc._1, enc._2)
  }

  /** Split an encoded read into super-mers, returning only the position and length of each.
   * @return an iterator of (position in sequence, minimizer rank, length of superkmer) */
  def superkmerPositions(encoded: NTBitArray): Iterator[Minimizer] = {
    val enc = scanner.allMatches(encoded)
    superkmerPositions(enc._1, enc._2)
  }

  /**
   * Split a read into superkmers, and return them together with the corresponding minimizer.
   *
   * @param encoded the read to split
   * @param matches discovered motif ranks in the superkmer
   * @return an iterator of (rank (hash/minimizer ID), encoded superkmer, location in sequence)
   */
  def splitRead(encoded: NTBitArray, matches: MinimizerPositions): Iterator[Supermer] = {
    val window = new PosRankWindow(priorities.width, k, matches)

    var regionStart = 0
    new Iterator[Supermer] {
      def hasNext: Boolean = window.hasNext

      def next(): Supermer = {
        val p = window.next

        //TODO INVALID handling for computed priorities
        if (!matches.isValid(p)) {
          throw new Exception(
            s"""|Found a window with no motif in a read. Is the supplied motif set valid?
                |Erroneous read without motif in a window: $encoded
                |Matches found: (TODO fill in this message)
                |""".stripMargin)
        }
        val rank = matches.data(p)

        var consumed = 1
        while (window.hasNext &&
          (window.head == p || window.motifRanks.equal(window.head, p))) {
          window.next
          consumed += 1
        }

        val thisStart = regionStart
        regionStart += consumed

        if (window.hasNext) {
          val segment = encoded.sliceAsCopy(thisStart, consumed + (k - 1))
          Supermer(rank, segment, thisStart)
        } else {
          val segment = encoded.sliceAsCopy(thisStart, encoded.size - thisStart)
          Supermer(rank, segment, thisStart)
        }
      }
    }
  }

  /**
   * Split a read into superkmers, returning their length and position with the corresponding minimizer.
   * @param encoded the read to split
   * @param matches discovered motif ranks in the superkmer
   * @return an iterator of (location in sequence, rank (hash/minimizer ID), length of supermer)
   */
  def superkmerPositions(encoded: NTBitArray, matches: MinimizerPositions): Iterator[Minimizer] = {
    val window = new PosRankWindow(priorities.width, k, matches)

    var regionStart = 0
    new Iterator[Minimizer] {
      def hasNext: Boolean = window.hasNext

      def next(): Minimizer = {
        val p = window.next

        if (!matches.isValid(p)) {
          throw new Exception(
            s"""|Found a window with no motif in a read. Is the supplied motif set valid?
                |Erroneous read without motif in a window: $encoded
                |Matches found: (TODO fill in this message)
                |""".stripMargin)
        }
        val rank = matches.data(p)

        var consumed = 1
        while (window.hasNext &&
          (window.head == p || window.motifRanks.equal(window.head, p))) {
          window.next
          consumed += 1
        }

        val thisStart = regionStart
        regionStart += consumed

        if (window.hasNext) {
          Minimizer(thisStart, rank, consumed + (k - 1))
        } else {
          Minimizer(thisStart, rank, encoded.size - thisStart)
        }
      }
    }
  }

  /** Split a read into super-mers, efficiently encoding them in binary form in the process,
   * also preserving sequence ID and location.
   * @param read The read to split
   * @param sequenceIDs IDs for each sequence title, to be preserved in the result
   * @return an iterator of superkmers with sequence ID and location populated
   */
  def splitEncodeLocation(read: InputFragment, sequenceIDs: Map[SeqTitle, SeqID]): Iterator[SplitSegment] = {
    val width = priorities.width * 2
    for {
      Supermer(rank, ntseq, location) <- splitEncode(read.nucleotides)
      shifted = rank(0) >>> (64 - width * 2)
    } yield SplitSegment(shifted, sequenceIDs(read.header), read.location + location, ntseq)
  }

  /** Compute a human-readable form of the bucket ID. */
  def humanReadable(id: BucketId): NTSeq =
    priorities.humanReadable(id)

}
