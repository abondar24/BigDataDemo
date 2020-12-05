package org.abondar.experimental.sparkdemo.java;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadJsonHive {

    public static void main(String[] args) {
        String warehouseLocation = "spark-warehouse";

        SparkSession session = SparkSession.builder().appName("Load Hive")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport().getOrCreate();

        Dataset input = session.read().json(args[0]);
        input.createOrReplaceTempView("test2");
        Dataset res = session.sql("SELECT * FROM test2");

        res.show();

        JavaRDD row1 = res.toJavaRDD().map((Function<Row,String>) row -> row.getString(0));

        System.out.println(row1);

    }
}
