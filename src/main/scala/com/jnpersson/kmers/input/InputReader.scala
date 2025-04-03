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

package com.jnpersson.kmers.input

import com.jnpersson.kmers.minimizer.InputFragment
import com.jnpersson.kmers.util.DNAHelpers
import com.jnpersson.kmers.{HDFSUtil, SeqLocation, SeqTitle}
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.language.postfixOps


sealed trait InputGrouping
/** Single reads or genomes */
case object Ungrouped extends InputGrouping
/** Paired-end reads */
case object PairedEnd extends InputGrouping

/**
 * A set of input files that can be parsed into [[InputFragment]]
 *
 * @param files files to read. A name of the format @list.txt will be parsed as a list of files.
 * @param k length of k-mers
 * @param maxReadLength max length of short sequences
 * @param inputGrouping whether input files are paired-end reads. If so, they are expected to appear in sequence, so that
 *                  the first file is a _1, the second a _2, the third a _1, etc.
 * @param spark the SparkSession
 */
class FileInputs(val files: Seq[String], k: Int, maxReadLength: Int, inputGrouping: InputGrouping = Ungrouped)(implicit spark: SparkSession) {
  protected val conf = new HConfiguration(spark.sparkContext.hadoopConfiguration)
  import spark.sqlContext.implicits._

  private val expandedFiles = files.toList.flatMap(f => {
    if (f.startsWith("@")) {
      println(s"Assuming that $f is a list of input files (using @ syntax)")
      val realName = f.drop(1)
      spark.read.textFile(realName).collect()
    } else {
      List(f)
    }
  })

  /**
   * By looking at the file name and checking for the presence of a .fai file in the case of fasta,
   * obtain an appropriate InputReader for a single file.
   */
  def forFile(file: String): InputReader = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file, max length $maxReadLength")
      new FastqShortInput(file, k, maxReadLength)
    } else {
      //Assume fasta format
      val faiPath = file + ".fai"
      if (HDFSUtil.fileExists(faiPath)) {
        println(s"$faiPath found. Using indexed fasta format for $file")
        new IndexedFastaInput(file, k)
      } else {
        println(s"$faiPath not found. Assuming simple fasta format for $file, max length $maxReadLength")
        new FastaShortInput(file, k, maxReadLength)
      }
    }
  }

  /**
   * By looking at the file name and checking for the presence of a .fai file in the case of fasta,
   * obtain an appropriate InputReader for a single file.
   * Read the files as paired-end reads if the second file was supplied.
   */
  def forPair(file: String, file2: String): InputReader = {
    println(s"Identified paired end inputs: $file, $file2")
    new PairedInputReader(forFile(file), forFile(file2))
  }

  /**
   * Parse all files in this set as InputFragments
   * @param withAmbiguous whether to include ambiguous nucleotides. If not, the inputs will be split and only valid
   *                      nucleotides retained.
   * @return
   */
  def getInputFragments(withAmbiguous: Boolean = false, sampleFraction: Option[Double] = None): Dataset[InputFragment] = {
    val readers = inputGrouping match {
      case PairedEnd =>
        if (files.size % 2 != 0) {
          throw new Exception(
            s"For paired end mode, please supply pairs of files (even number). ${files.size} files were supplied")
        }
        expandedFiles.grouped(2).map(pair => forPair(pair(0), pair(1))).toList
      case _ =>
        expandedFiles.map(forFile)
    }
    val fs = readers.map(_.getInputFragments(withAmbiguous, sampleFraction))
    spark.sparkContext.union(fs.map(_.rdd)).toDS()
  }

  /**
   * All sequence titles contained in this set of input files
   */
  def getSequenceTitles: Dataset[SeqTitle] = {
    val titles = expandedFiles.map(forFile(_)).map(_.getSequenceTitles)
    spark.sparkContext.union(titles.map(_.rdd)).toDS()
  }
}

object InputReader {
  def addRCFragments(fs: Dataset[InputFragment])(implicit spark: SparkSession): Dataset[InputFragment] = {
    import spark.sqlContext.implicits._
    fs.flatMap(r =>
      List(r, r.copy(
        nucleotides = DNAHelpers.reverseComplement(r.nucleotides),
        nucleotides2 = r.nucleotides2.map(n2 => DNAHelpers.reverseComplement(n2))
      ))
    )
  }
}

/**
 * A sequence input converter that reads data from one file (or a pair of paired-end files) using a specific
 * Hadoop format, making the result available as Dataset[InputFragment]
 * @param file the file to read
 * @param k length of k-mers
 */
abstract class InputReader(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._


  //Multiline DNA sequences may contain intermittent newlines but should
  //still be treated as a single valid match.

  private val validBases = "[ACTGUactgu][ACTGUactgu\n\r]*".r

  /**
   * Split the fragments around unknown or invalid characters.
   */
  private def removeInvalid(data: Dataset[InputFragment]): Dataset[InputFragment] = {
    val valid = this.validBases
    data.flatMap(x => {
      valid.findAllMatchIn(x.nucleotides).map(m => {
        x.copy(nucleotides = m.matched, location = x.location + m.start)
      })
    })
  }

  /**
   * Sequence titles in this file
   * @return
   */
  def getSequenceTitles: Dataset[SeqTitle]

  /**
   * Read sequence data as fragments from the input files, removing any newlines.
   * @return
   */
  protected[input] def getFragments(): Dataset[InputFragment]

  /**
   * Load sequence fragments from files, optionally adding reverse complements and/or sampling.
   */
  def getInputFragments(withAmbiguous: Boolean,
                        sampleFraction: Option[Double]): Dataset[InputFragment] = {
    val raw = getFragments()
    val valid = if (withAmbiguous) raw else removeInvalid(raw)

    //Note: could possibly push down sampling even deeper
    sampleFraction match {
      case None => valid
      case Some(f) => valid.sample(f)
    }
  }
}

/** Transforms two input readers (R1 and R2 lanes) into a reader of paired-end reads */
class PairedInputReader(lhs: InputReader, rhs: InputReader)(implicit spark: SparkSession) extends InputReader {
  import spark.sqlContext.implicits._

  protected[input] def getFragments: Dataset[InputFragment] = {
    def removeSuffix(f: InputFragment, suffix: String) =
      f.copy(header = f.header.replaceAll(suffix + "$", ""))

    /* As we currently have no input format that correctly handles paired reads, joining the reads by
          header is the best we can do (and still inexpensive in the big picture).
          Otherwise, it is hard to guarantee that they would be paired up correctly.
          We remove the /1 and /2 suffixes from the headers if they are present, as they would break the join.
        */
    val fr1 = lhs.getFragments().map(x => removeSuffix(x, "/1"))
    val fr2 = rhs.getFragments().map(x => removeSuffix(x, "/2"))
    fr1.joinWith(fr2, fr1("header") === fr2("header")).map(pair =>
      pair._1.copy(nucleotides2 = Some(pair._2.nucleotides)))
  }

  override def getSequenceTitles: Dataset[SeqTitle] =
    lhs.getSequenceTitles
}
