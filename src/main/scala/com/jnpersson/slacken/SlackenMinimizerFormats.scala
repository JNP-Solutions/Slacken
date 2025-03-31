/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.minimizer.{All, MinSplitter, MinimizerPriorities, RandomXOR, XORMask}
import com.jnpersson.kmers.{MinimizerFormats, RandomXORFormat, SparkConfiguration, SplitterFormat}

/** Minimizer formats supported by Slacken. */
object SlackenMinimizerFormats extends MinimizerFormats[SlackenConf] {
  protected val formatsById = Map[String, SplitterFormat[_]](
    "randomXOR" -> new RandomXORFormat())

  protected val formatsByCls = Map[Class[_], SplitterFormat[_]](
    classOf[RandomXOR] -> new RandomXORFormat())

  def makeSplitter(config: SlackenConf): MinSplitter[_ <: MinimizerPriorities] = {
    config.requireSuppliedK()
    val m = config.minimizerWidth()
    config.ordering() match {
      case XORMask(mask, canonical) =>
        //computed RandomXOR for a wide m
        MinSplitter(RandomXOR(m, mask, canonical = canonical), config.k())
      case _ => ???
    }
  }
}
