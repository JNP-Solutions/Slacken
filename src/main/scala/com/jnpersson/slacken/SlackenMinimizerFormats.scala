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

package com.jnpersson.slacken

import com.jnpersson.kmers.minimizer.{All, MinSplitter, MinimizerPriorities, RandomXOR, XORMask}
import com.jnpersson.kmers.{MinimizerCLIConf, MinimizerFormats, RandomXORFormat, SparkConfiguration, SplitterFormat}

/** Minimizer formats supported by Slacken. */
object SlackenMinimizerFormats extends MinimizerFormats[MinimizerCLIConf] {
  protected val formatsById = Map[String, SplitterFormat[_]](
    "randomXOR" -> new RandomXORFormat())

  protected val formatsByCls = Map[Class[_], SplitterFormat[_]](
    classOf[RandomXOR] -> new RandomXORFormat())

  def makeSplitter(config: MinimizerCLIConf): MinSplitter[_ <: MinimizerPriorities] = {
    config.requireSuppliedK()
    val m = config.minimizerWidth()
    val k = config.k()
    config.ordering() match {
      case XORMask(mask, canonical) =>
        //computed RandomXOR for a wide m
        val s1 = MinSplitter(RandomXOR(m, mask, canonical = canonical), k)
        MinSplitter(config.seedMask(s1.priorities), k)
      case _ => ???
    }
  }
}
