package com.jnpersson.slacken

import it.unimi.dsi.fastutil.Hash

final class KmerStrategy1 extends Hash.Strategy[Array[Long]] {

  override def hashCode(minimizer: Array[Long]): Int = 31 + (minimizer(0) ^ (minimizer(0) >>> 32)).toInt

  override def equals(m1: Array[Long], m2: Array[Long]): Boolean = {
    // Object2IntCustomHashMap.getInt does null comparisons by passing null values to m2.
    // We ensure that m1 is never null
    m2 != null && m1(0) == m2(0)
  }

}

final class KmerStrategy2 extends Hash.Strategy[Array[Long]] {

  override def hashCode(minimizer: Array[Long]): Int = {
    31 * (31 +
      (minimizer(0) ^ (minimizer(0) >>> 32)).toInt) +
      (minimizer(1) ^ (minimizer(1) >>> 32)).toInt
  }

  override def equals(m1: Array[Long], m2: Array[Long]): Boolean = {

    m2 != null &&
      m1(0) == m2(0) &&
      m1(1) == m2(1)
  }

}

final class KmerStrategy3 extends Hash.Strategy[Array[Long]] {

  override def hashCode(minimizer: Array[Long]): Int = {
    31 * (31 * (31 +
      (minimizer(0) ^ (minimizer(0) >>> 32)).toInt) +
      (minimizer(1) ^ (minimizer(1) >>> 32)).toInt) +
      (minimizer(2) ^ (minimizer(2) >>> 32)).toInt
  }

  override def equals(m1: Array[Long], m2: Array[Long]): Boolean = {

    m2 != null &&
      m1(0) == m2(0) &&
      m1(1) == m2(1) &&
      m1(2) == m2(2)
  }

}

final class KmerStrategy4 extends Hash.Strategy[Array[Long]] {

  override def hashCode(minimizer: Array[Long]): Int = {
    31 * (31 * (31 * (31 +
      (minimizer(0) ^ (minimizer(0) >>> 32)).toInt) +
      (minimizer(1) ^ (minimizer(1) >>> 32)).toInt) +
      (minimizer(2) ^ (minimizer(2) >>> 32)).toInt) +
      (minimizer(3) ^ (minimizer(3) >>> 32)).toInt
  }

  override def equals(m1: Array[Long], m2: Array[Long]): Boolean = {

    m2 != null &&
      m1(0) == m2(0) &&
      m1(1) == m2(1) &&
      m1(2) == m2(2) &&
      m1(3) == m2(3)
  }

}

