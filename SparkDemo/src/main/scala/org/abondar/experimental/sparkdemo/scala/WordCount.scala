package org.abondar.experimental.sparkdemo.scala

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word,1)).reduceByKey{case(x,y) => x + y}

    counts.saveAsTextFile(args(1))
  }

}
