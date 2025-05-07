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
import scala.collection.JavaConverters._


object ReportWriterFactory {
  /** A writer that writes Slacken taxonomic classification reports using the SeqsLab plugin API.
   * The Dataframe to be written is assumed to be created by [[Slacken.classifyReads()]].
    */
  private class ReportWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val dataSource: DataSource = operatorCtx.getDataSource()
    val url: String = dataSource.getUrl()

    override def init(): Writer = this

    override def getDataSource: DataSource = dataSource

    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val className = this.getClass.getSimpleName
      val properties = operatorCtx.getProperties.asScala

      val delimiter = properties.get(s"$className:delimiter").map(_.asInstanceOf[String]).getOrElse(",")
      val header = properties.get(s"$className:header").map(_.asInstanceOf[String]).getOrElse("true")
      val partitionNum = properties.get(s"$className:partitionNum").map(_.asInstanceOf[String].toInt).getOrElse(1)

      val slacken: Slacken = ??? //get from somewhere
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