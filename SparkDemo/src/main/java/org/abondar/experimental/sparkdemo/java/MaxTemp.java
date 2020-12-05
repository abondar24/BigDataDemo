package org.abondar.experimental.sparkdemo.java;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class MaxTemp {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext("local", "MaxTemp", conf);
        JavaRDD<String> lines = context.textFile(args[0]);
        JavaRDD<String[]> records = lines.map((Function<String, String[]>) s -> s.split("\t"));
        JavaRDD<String[]> filtered = records.filter((Function<String[], Boolean>) rec ->
                rec[1] != "9999" && rec[2].matches("[01459]"));
        JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(
                (PairFunction<String[], Integer, Integer>) rec ->
                        new Tuple2<>(Integer.parseInt(rec[0]), Integer.parseInt(rec[1])));
        JavaPairRDD<Integer,Integer> maxTemps = tuples.reduceByKey((Function2<Integer, Integer, Integer>) Math::max);

        maxTemps.saveAsTextFile(args[1]);
    }
}
