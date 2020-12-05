package org.abondar.experimental.sparkdemo.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamFromConsole {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stream from console")
    val ssc = new StreamingContext(conf,Seconds(2))

    val lines = ssc.socketTextStream("172.17.0.1", 7777)
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
