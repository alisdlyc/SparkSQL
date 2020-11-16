package com.alisdlyc.SparkZkpk

import org.apache.spark.{SparkConf, SparkContext}

object InvertIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("index_revert").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = List(
      "id1 the spark",
      "id2 the hdfs",
      "id3 hbase",
      "id4 spark",
      "id5 hive",
      "id6 hive and spark",
      "id7 the yarn"
    )
    val rdd = sc.parallelize(input).flatMap({
      line =>
        val arr = line.split(" ", 2)
        arr(1).split(" ").map(word => (arr(0), word))
    })
    val re = rdd.map(kv => (kv._2, kv._1)).reduceByKey(_ + " " + _)
    re.collect().foreach(println)
    re.saveAsTextFile("hdfs://master:9000/sparktest/invertindex")
    sc.stop()
  }

}
