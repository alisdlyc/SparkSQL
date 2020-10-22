package com.alisdlyc.sparkrdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Pai")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 20000
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    sc.stop()
  }
}
