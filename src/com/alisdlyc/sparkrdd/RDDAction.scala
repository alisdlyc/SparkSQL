package com.alisdlyc.sparkrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDAction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array(
      ("zhangsan", 18), ("lisi", 19), ("wangwu", 20), ("zhangsan", 18), ("lisi", 19), ("wangwu", 20), ("maliu", 21)), 3)

    rdd1.foreachPartition(iter => {
      iter.foreach(println)
    })
    //    rdd1.mapPartitions(iter=>{
    //      println("插入数据库。。。")
    //      iter
    //    }, true).collect()

    //    val result = rdd1.distinct()
    //        result.foreach(println)

    //    val rdd2 = sc.makeRDD(Array(("zhangsan",18),("lisi",19),("wangwu",20),("maliu1",21)))
    //    rdd1.intersection(rdd2).foreach(println)
    //    rdd2.subtract(rdd1).foreach(println)

    //    val rdd2 = sc.makeRDD(Array(("zhangsan",100),("lisi",200),("wangwu",300),("tianqi",400)))

    //    val result = rdd1.union(rdd2)
    //    result.foreach(println)
    //    val result = rdd1.rightOuterJoin(rdd2)
    //    result.foreach(println)
    sc.stop()
  }
}
