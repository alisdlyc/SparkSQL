package com.alisdlyc.sparkrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDPipeline {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Spark Pipeline")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a", "b", "c"))
    val rdd1 = rdd.map(name => {
      println("*** map " + name)
      name + " qwq"
    })

    val rdd2 = rdd1.filter(name => {
      println("--- filter " + name)
      true
    })

    rdd2.foreach(println)
    sc.stop()
  }
}
