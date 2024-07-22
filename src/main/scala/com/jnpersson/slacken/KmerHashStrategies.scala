package com.jnpersson.slacken

import it.unimi.dsi.fastutil.Hash
class KmerStrategy1 extends Hash.Strategy[Array[Long]]{

  override def hashCode(minimizer: Array[Long]): Int = 31 + (minimizer(0) ^ (minimizer(0) >>> 32)).toInt

  override def equals(m1: Array[Long] , m2: Array[Long] ): Boolean = {
    if (m1 != null && m2 != null) m1(0) == m2(0)
    else if (m1 == null && m2 == null) true else false
  }

}

class KmerStrategy2 extends Hash.Strategy[Array[Long]]{

  override def hashCode(minimizer: Array[Long]): Int = {
    31*(31+ (minimizer(0) ^ (minimizer(0) >>> 32)).toInt) + (minimizer(1) ^ (minimizer(1) >>> 32)).toInt
  }

  override def equals(m1: Array[Long] , m2: Array[Long] ): Boolean = {

    if (m1 != null && m2 != null) (m1(0) == m2(0)) && (m1(1)==m2(1))
    else if (m1 == null && m2 == null) true else false
  }

}

class KmerStrategy3 extends Hash.Strategy[Array[Long]]{

  override def hashCode(minimizer: Array[Long]): Int = {
    31*(31*(31+ (minimizer(0) ^ (minimizer(0) >>> 32)).toInt) + (minimizer(1) ^ (minimizer(1) >>> 32)).toInt)
    + (minimizer(2) ^ (minimizer(2) >>> 32)).toInt
  }

  override def equals(m1: Array[Long] , m2: Array[Long] ): Boolean = {

    if (m1 != null && m2 != null) (m1(0) == m2(0)) && (m1(1)==m2(1)) && (m1(2)==m2(2))
    else if (m1 == null && m2 == null) true else false
  }

}

class KmerStrategy4 extends Hash.Strategy[Array[Long]]{

  override def hashCode(minimizer: Array[Long]): Int = {
    31*(31*(31*(31+ (minimizer(0) ^ (minimizer(0) >>> 32)).toInt) + (minimizer(1) ^ (minimizer(1) >>> 32)).toInt)
    + (minimizer(2) ^ (minimizer(2) >>> 32)).toInt) + (minimizer(3) ^ (minimizer(3) >>> 32)).toInt
  }

  override def equals(m1: Array[Long] , m2: Array[Long] ): Boolean = {

    if (m1 != null && m2 != null) (m1(0) == m2(0)) && (m1(1)==m2(1)) && (m1(2)==m2(2)) && (m1(3)==m2(3))
    else if (m1 == null && m2 == null) true else false
  }

}

