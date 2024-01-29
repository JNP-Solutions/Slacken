/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.spark.{Commands, Configuration, RunCmd, SparkTool}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand

class SlackenConf(args: Array[String]) extends Configuration(args) {
  version(s"Slacken ${getClass.getPackage.getImplementationVersion} beta (c) 2019-2023 Johan Nyström-Persson")
  banner("Usage:")

  val taxonIndex = new Subcommand("taxonIndex") {
    val location = opt[String](required = true, descr = "Path to location where index is stored")
    val taxonomy = opt[String](descr = "Path to taxonomy directory (nodes.dmp and names.dmp)", required = true)

    def index(implicit spark: SparkSession): SupermerIndex =
      SupermerIndex.load(location(), taxonomy())

    def histogram = new RunCmd("histogram") {
      val output = opt[String](descr = "Output location", required = true)

      def run(implicit spark: SparkSession): Unit = {
        index.writeDepthHistogram(output())
      }
    }
    addSubcommand(histogram)

    val build = new RunCmd("build") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val labels = opt[String](descr = "Path to sequence taxonomic label file", required = true)

      def run(implicit spark: SparkSession): Unit = {
        val dc = discount
        val i = SupermerIndex.empty(dc, taxonomy(), inFiles())
        val bkts = i.makeBuckets(dc, inFiles(), labels(), true)
        i.writeBuckets(bkts, location())
      }
    }
    addSubcommand(build)

    val classify = new RunCmd("classify") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val paired = opt[Boolean](descr = "Inputs are paired-end reads", default = Some(false))
      val unclassified = toggle(descrYes = "Output unclassified reads", default = Some(true))
      val output = opt[String](descr = "Output location", required = true)

      def cpar = ClassifyParams(2, 0, unclassified())
      def run(implicit spark: SparkSession): Unit = {
        val i = index
        val d = discount(i.params)
        val input = d.inputReader(paired(), inFiles(): _*).getInputFragments(withRC = false, withAmbiguous = true)
        i.classifyAndWrite(input, output(), cpar)
      }
    }
    addSubcommand(classify)

    val stats = new RunCmd("stats") {
      def run(implicit spark: SparkSession): Unit = {
        index.showIndexStats()
      }
    }
    addSubcommand(stats)
  }
  addSubcommand(taxonIndex)

  verify()
}

/** Implements the Kraken 1 method for taxonomic classification. */
object Slacken extends SparkTool("Slacken") {
  def main(args: Array[String]): Unit = {
    val conf = new SlackenConf(args)
    Commands.run(new SlackenConf(args))(sparkSession(conf))
  }
}
