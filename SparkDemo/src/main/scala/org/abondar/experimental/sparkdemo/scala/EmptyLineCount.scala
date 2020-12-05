package org.abondar.experimental.sparkdemo.scala

import org.apache.spark.{SparkConf, SparkContext}


object EmptyLineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EmptyLineCount")
    val sc = new SparkContext(conf)

    val file = sc.textFile(args(0))
    val blankLines = sc.longAccumulator("lines")

    val callSigns = file.flatMap(line => {
      if (line == ""){
        blankLines.add(1)
      }
      line.split(" ")
    })

    callSigns.saveAsTextFile("/home/output")
    println("Blank lines: " + blankLines.value)
  }

}
