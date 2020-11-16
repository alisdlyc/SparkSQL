package com.alisdlyc.SparkZkpk

import org.apache.spark.{SparkConf, SparkContext}

class SparkWordCount {

}

object SparkWordCount{
  def main(args: Array[String]): Unit = {
    val list = List("hello hi hi spark",
    "hello spark hello hi sparksql",
    "hello hi hi sparkstreaming",
    "hello hi sparkgraphx")

    val sparkConf = new SparkConf().setAppName("word-count").setMaster("local[*]")
    val sc = new SparkContext(sparkConf);
    val lines = sc.parallelize(list)
    val words = lines.flatMap(_.split(" "))
    val wordAndOne = words.map((_,1))
    val wordAndNum = wordAndOne.reduceByKey(_+_)

    val ret = wordAndNum.sortBy(_._2, false)
    println(ret.collect().mkString(","))
    ret.saveAsTextFile("hdfs://master:9000/sparktest/wordcount")
    sc.stop()
  }
}