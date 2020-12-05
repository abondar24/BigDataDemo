package org.abondar.experimental.sparkdemo.java;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamFromConsole {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
        JavaDStream<String> errorLines = lines.filter((Function<String, Boolean>) line -> line.contains("error"));
        errorLines.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
