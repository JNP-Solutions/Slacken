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

import com.jnpersson.fastdoop.{FASTAshortInputFileFormat, FASTQInputFileFormat, IndexedFastaFormat, PartialSequence, QRecord, Record}
import com.jnpersson.kmers.{HDFSUtil, SeqLocation, SeqTitle}
import com.jnpersson.kmers.minimizer.InputFragment
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


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

  /** Clone this Inputs with a different value of k. */
  def withK(newK: Int): FileInputs =
    new FileInputs(files, newK, maxReadLength, inputGrouping)

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
  def forPair(file: String, file2: String): PairedInputReader = {
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


/**
 * Parser for Fastdoop records.
 * Splits longer sequences into fragments of a controlled maximum length, optionally sampling them.
 *
 * @param k length of k-mers
 */
final case class FragmentParser(k: Int) {
  def makeInputFragment(header: SeqTitle, location: SeqLocation, buffer: Array[Byte],
                        start: Int, end: Int): InputFragment = {
    val nucleotides = new String(buffer, start, end - start + 1)
    InputFragment(header, location, nucleotides, None)
  }

  val FIRST_LOCATION = 1

  /** Convert a record of a supported type to InputFragment, making any necessary transformations on the way
   * to guarantee preservation of all k-mers.
   */
  def toFragment(record: AnyRef): InputFragment = record match {
    case rec: Record =>
      makeInputFragment(rec.getKey.split(" ")(0), FIRST_LOCATION, rec.getBuffer,
        rec.getStartValue, rec.getEndValue)
    case qrec: QRecord =>
      makeInputFragment(qrec.getKey.split(" ")(0), FIRST_LOCATION, qrec.getBuffer,
        qrec.getStartValue, qrec.getEndValue)
    case partialSeq: PartialSequence =>
      val kmers = partialSeq.getBytesToProcess
      val start = partialSeq.getStartValue
      if (kmers == 0) {
        return InputFragment("", 0, "", None)
      }

      val extensionPart = new String(partialSeq.getBuffer, start + kmers, k - 1)
      val newlines = extensionPart.count(_ == '\n')

      //Newlines will be removed eventually, however we have to compensate for them here
      //to include all k-mers properly
      //Note: we assume that the extra part does not contain newlines itself

      //Although this code is general, for more than one newline in this part (as the case may be for a large k),
      //deeper changes to Fastdoop may be needed.
      //This value is 0-based inclusive of end
      val end = start + kmers - 1 + (k - 1) + newlines
      val useEnd = if (end > partialSeq.getEndValue) partialSeq.getEndValue else end

      val key = partialSeq.getKey.split(" ")(0)
      makeInputFragment(key, partialSeq.getSeqPosition, partialSeq.getBuffer, start, useEnd)
  }
}

/**
 * A sequence input converter that reads data from one file using a specific
 * Hadoop format, making the result available as Dataset[InputFragment]
 * @param file the file to read
 * @param k length of k-mers
 */
abstract class HadoopInputReader[R <: AnyRef](file: String, k: Int)(implicit spark: SparkSession) extends InputReader {
  import spark.sqlContext.implicits._
  protected val conf = new HConfiguration(sc.hadoopConfiguration)

  //Fastdoop parameter for correct overlap between partial sequences
  conf.set("k", k.toString)

  protected def loadFile(input: String): RDD[R]
  protected def rdd: RDD[R] = loadFile(file)
  protected[kmers] val parser = FragmentParser(k)

  protected[input] def getFragments(): Dataset[InputFragment] = {
    val p = parser
    rdd.map(p.toFragment).toDS()
  }
}

/**
 * Input reader for FASTA sequences of a fixed maximum length.
 * Uses [[FASTAshortInputFileFormat]]
 * @param file the file to read
 * @param k length of k-mers
 * @param maxReadLength maximum length of a single read
 * @param file2 second file for paired-end reads
 */
class FastaShortInput(file: String, k: Int, maxReadLength: Int)
                     (implicit spark: SparkSession) extends HadoopInputReader[Record](file, k) {
  import spark.sqlContext.implicits._

  private val bufsiz = maxReadLength + // sequence data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  protected def loadFile(input: String): RDD[Record] =
    sc.newAPIHadoopFile(input, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record], conf).values

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(_.getKey).toDS().distinct()
}

/**
 * Input reader for FASTQ short reads. Uses [[FASTQInputFileFormat]]
 * @param file the file to read
 * @param k length of k-mers
 * @param maxReadLength maximum length of a single read
 * @param file2 second file for paired-end reads
 */
class FastqShortInput(file: String, k: Int, maxReadLength: Int)
                     (implicit spark: SparkSession) extends HadoopInputReader[QRecord](file, k) {
  import spark.sqlContext.implicits._

  private val bufsiz = maxReadLength * 2 + // sequence and quality data
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  protected def loadFile(input: String): RDD[QRecord] =
    sc.newAPIHadoopFile(input, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord], conf).values

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(_.getKey).toDS().distinct()
}

/**
 * Input reader for FASTA files containing potentially long sequences, with a .fai index
 * FAI indexes can be created with tools such as seqkit.
 * Uses [[IndexedFastaFormat]]
 *
 * @param file the file to read
 * @param k length of k-mers
 */
class IndexedFastaInput(file: String, k: Int)(implicit spark: SparkSession)
  extends HadoopInputReader[PartialSequence](file, k) {
  import spark.sqlContext.implicits._

  protected def loadFile(input: String): RDD[PartialSequence] =
    sc.newAPIHadoopFile(input, classOf[IndexedFastaFormat], classOf[Text], classOf[PartialSequence], conf).values

  def getSequenceTitles: Dataset[SeqTitle] =
    rdd.map(_.getKey).toDS().distinct()
}
