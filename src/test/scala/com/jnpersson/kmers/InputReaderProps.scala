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

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer.InputFragment
import org.scalacheck.{Gen, Shrink}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

class InputReaderProps extends AnyFunSuite with SparkSessionTestWrapper with ScalaCheckPropertyChecks {
  import TestGenerators._

  implicit val sp = spark

  val k = 35
  val maxReadLength = 100000

  // Write a new temporary file with content
  def generateFile(content: String): String = {
    val loc = Files.createTempFile(null, ".fasta")
    Files.writeString(loc, content)
    println(loc)
    loc.toString
  }

  //Read files using InputReader
  def readFiles(files: Seq[String]): Inputs =
    new Inputs(files, k, maxReadLength)

  //Delete a temporary file
  def removeFile(file: String): Unit = {
    new java.io.File(file).delete()
  }

  def removeSeparators(x: String): String =
    x.replaceAll("[\n\r]+", "")

  //File format generators
  case class SeqRecordFile(records: List[(InputFragment, String)], lineSeparator: String) {
    override def toString: String =
      records.map(r => s">${r._1.header}\n${r._2}").mkString("\n")

    def fileString: String =
      records.map(r => s">${r._1.header}$lineSeparator${r._2}").mkString(lineSeparator)
  }

  implicit def shrinkFile: Shrink[SeqRecordFile] =
    Shrink { case x =>
      Shrink.shrink(x.records).map(recs => SeqRecordFile(recs, x.lineSeparator))
    }

  //lines have different length, to simulate a complex fasta file
  //Triples of (file data, expected input fragment, line separator)
  def fastaFileShortSequences(k: Int, lineSep: String): Gen[(String, InputFragment, String)] =
    for {
      i <- Gen.choose(1, 10)
      dnaSeqs <- Gen.listOfN(i, dnaStrings(k, 100))
      id <- Gen.stringOfN(10, Gen.alphaNumChar)
      sequence = dnaSeqs.mkString("")
      record = dnaSeqs.mkString(lineSep)
      fragment = InputFragment(id, 0, sequence, None)
    } yield (record, fragment, lineSep)

  //Fasta files, pairs of data and expected InputFragment records
  def fastaFiles(k: Int): Gen[SeqRecordFile] =
    for {
      lineSep <- Gen.oneOf(List("\n", "\n\r"))
      n <- Gen.choose(1, 10)
      records <- Gen.listOfN(n, fastaFileShortSequences(k, lineSep))
      recordData = records.map(_._1)
      recordFragments = records.map(_._2)
    } yield SeqRecordFile(recordFragments zip recordData, lineSep)


  test("fasta short reads fragment reading") {
    forAll(fastaFiles(k)) { case file =>
      val loc = generateFile(file.fileString)
      val inputs = readFiles(List(loc))
      val fragments = file.records.map(pair => (pair._1.header, pair._1.nucleotides)).sortBy(_._1)
      val got = inputs.getInputFragments(withRC = false).collect().toList.sortBy(_.header).map(r =>
        (r.header, removeSeparators(r.nucleotides))
      )
      got should equal(fragments)
      removeFile(loc)
    }
  }

}
