package com.alisdlyc.sparkrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDMake {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RDDMaker")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5), ("a", 1)), 2)
    rdd1.foreach(println)


    val rdd2 = sc.makeRDD(Array(("a", 2), ("b", 44), ("a", 2)), 3)
    rdd2.distinct().foreach(println) // 去重

    val result = rdd1.join(rdd2)
    result.foreach(println)

    val result2 = rdd1.leftOuterJoin(rdd2)
    result2.foreach(println)

    rdd1.union(rdd2)

    rdd1.intersection(rdd2) // 取交集
    rdd1.subtract(rdd2) // 取差集
    rdd2.subtract(rdd1) // 取差集

    rdd1.mapPartitions(iter => {
      println("qwq")
      iter
    }, true).collect()
    // 按照分区操作partition

    rdd1.foreachPartition(iter => {
      iter.foreach(println)
    })


  }

}
