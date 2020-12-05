package org.abondar.experimental.sparkdemo.scala

import org.apache.spark.sql.SparkSession


object LoadJsonHive {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)


    val warehouseLocation = "spark-warehouse"
    val session = SparkSession.builder().appName("Load Hive")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport().getOrCreate()

    val input = session.read.json(inputFile)
    input.printSchema()

    input.createOrReplaceTempView("test1")
    val res = session.sql("SELECT * FROM test1")
    println(res.show())

    val row1 = res.rdd.map(row => row.getString(1)).collect()
    println(row1)

  }
}
