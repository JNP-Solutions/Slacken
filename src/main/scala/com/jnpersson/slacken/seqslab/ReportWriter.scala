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

package com.jnpersson.slacken.seqslab

import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.jnpersson.slacken.Slacken
import com.jnpersson.slacken.seqslab.ReportWriterFactory.ReportWriter
import org.apache.spark.sql.{Dataset, Row}

import java.lang


object ReportWriterFactory {
  /** A writer that writes Slacken taxonomic classification reports using the SeqsLab plugin API.
   * The Dataframe to be written is assumed to be created by [[Slacken.classifyReads()]].
    */
  private class ReportWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val dataSource: DataSource = operatorCtx.getDataSource()

    override def init(): Writer = this

    override def getDataSource: DataSource = dataSource

    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val taskFqn = operatorCtx.getTaskFqn
      val piperContext = pluginCtx.piper

      val indexLocation = piperContext.getInput(s"$taskFqn.indexLocation").getString
      val confidence = piperContext.getInput(s"$taskFqn.confidence").getDouble
      val minHitGroups = piperContext.getInput(s"$taskFqn.minHitGroups").getInteger
      val withUnclassified = piperContext.getInput(s"$taskFqn.withUnclassified").getBoolean

      val perReadOutput = false
      val sampleRegex = None //not needed for report writing
      implicit val spark = piperContext.spark

      val slacken = new SlackenSeqslab(indexLocation, perReadOutput, sampleRegex,
        confidence, minHitGroups, withUnclassified)
      slacken.writeReports(t1, dataSource.getUrl)

      null
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

/** Builds Slacken report writers for the SeqsLab plugin API.
 */
class ReportWriterFactory extends WriterSupport {

  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer =
    new ReportWriter(pluginContext, operatorContext)
}