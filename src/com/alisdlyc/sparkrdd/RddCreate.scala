package com.alisdlyc.sparkrdd

import org.apache.spark.{SparkConf, SparkContext}

object RddCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RDD Create")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("./src/com/alisdlyc/sparkrdd/qwq.txt")
    rdd1.foreach(println)
    val rdd2 = rdd1.map(s=>s.length).reduce((a, b) => a + b)
    println("------- 共有 " + rdd2 + " 个字符--------")
    val rdd3 = rdd1.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    // rdd3.saveAsTextFile("./src/com/alisdlyc/sparkrdd/qwq-re.txt")
    rdd3.foreach(println)

    val count = sc.parallelize(1 to Int.MaxValue).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / Int.MaxValue}")



    sc.stop()
  }

}
