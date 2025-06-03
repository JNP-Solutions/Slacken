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

import com.atgenomix.seqslab.piper.plugin.api.writer.WriterSupport
import com.atgenomix.seqslab.piper.plugin.api.{PiperContext, PiperPlugin, PluginContext}
import scala.collection.JavaConverters._

import java.util

/** Slacken plugin for SeqsLab. */
class SlackenPlugin extends PiperPlugin {

  override def init(context: PiperContext): PluginContext = super.init(context)

  override def registerWriters(): util.Map[String, WriterSupport] =
    Map[String, WriterSupport]("ReportWriter" -> new ReportWriterFactory).asJava

}

