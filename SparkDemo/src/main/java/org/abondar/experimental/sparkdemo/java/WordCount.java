package org.abondar.experimental.sparkdemo.java;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;


public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(args[0]);

        JavaRDD<String> words = input.flatMap((FlatMapFunction<String, String>) x ->
                Arrays.asList(x.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts = words.mapToPair((PairFunction<String, String, Integer>) x ->
                new Tuple2<>(x, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        counts.saveAsTextFile(args[1]);
    }
}
