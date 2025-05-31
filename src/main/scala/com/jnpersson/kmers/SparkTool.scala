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

package com.jnpersson.kmers

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import com.jnpersson.kmers.input.{FileInputs, InputGrouping, Ungrouped}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.security.SpillCallBackPathsFinder
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

/** A Spark-based tool.
 * @param appName Name of the application */
private[jnpersson] abstract class SparkTool(appName: String) {

  /** Create a SparkSession with the default settings */
  def sparkSession(): SparkSession = {
    val sp = SparkSession.builder().appName(appName).
      enableHiveSupport().
      getOrCreate()

    //BareLocalFileSystem bypasses the need for winutils.exe on Windows and does no harm on other OS's
    //This affects access to file:/ paths (effectively local files)
    sp.sparkContext.hadoopConfiguration.
      setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])
    sp
  }

  def handleScallopException(se: ScallopExitException): Unit = se match {
    case ScallopExitException(0) =>
    //Scallop tried to exit, clean return. Do not call System.exit as we may be in a Spark driver
    case se@ScallopExitException(code) =>
      System.err.println(s"Exit code $code")
      throw se
  }
}

object SparkTool {
  /** Create a new SparkSession with the given number of shuffle partitions */
  def newSession(base: SparkSession, buckets: Int): SparkSession = {
    val session = base.newSession()
    session.conf.set("spark.sql.shuffle.partitions", buckets.toString)
    session
  }
}

trait HasInputReader {
  this: ScallopConf =>

  protected def defaultMaxSequenceLength = 10000000 //10M bps
  val maxSequenceLength = opt[Int](name = "maxlen",
    descr = s"Maximum length of a single short sequence/read (default $defaultMaxSequenceLength)",
    default = Some(defaultMaxSequenceLength),
    hidden = true)

  def inputReader(files: Seq[String], k: Int, grouping: InputGrouping)(implicit spark: SparkSession) =
    new FileInputs(files, k, maxSequenceLength(), grouping)
}

/**
 * CLI configuration for a Spark-based application.
 */
//noinspection TypeAnnotation
class SparkConfiguration(args: Array[String])(implicit val spark: SparkSession) extends ScallopConf(args) {
  val partitions =
    opt[Int](descr = "Number of shuffle partitions/parquet buckets for indexes (default 200)", default = Some(200))

  def finishSetup(): this.type = {
    verify()
    spark.conf.set("spark.sql.shuffle.partitions", partitions())
    this
  }
}
