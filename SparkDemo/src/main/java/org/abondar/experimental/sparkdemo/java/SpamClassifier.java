package org.abondar.experimental.sparkdemo.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.Arrays;


public class SpamClassifier {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext("local", "Spam Classifier", conf);

        JavaRDD<String> spam = context.textFile(args[0]);
        JavaRDD<String> normal = context.textFile(args[1]);

        final HashingTF tf  = new HashingTF(10000);

        JavaRDD<LabeledPoint> posExamples = spam.map((Function<String,LabeledPoint>) email ->
                new LabeledPoint(1,tf.transform(Arrays.asList(email.split(" ")))));

        JavaRDD<LabeledPoint> negExamples = normal.map((Function<String,LabeledPoint>) email ->
                new LabeledPoint(0,tf.transform(Arrays.asList(email.split(" ")))));

        JavaRDD<LabeledPoint> trainData = posExamples.union(negExamples);
        trainData.cache();

        LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());

        Vector posTest = tf.transform(Arrays.asList("O M G GET cheap stuff by sending money to ..."
                .split(" ")));
        Vector negTest = tf.transform(Arrays.asList("Hi Dad, I started studying Spark the other ..."
                .split(" ")));

        System.out.println("Prediction for positive test: " + model.predict(posTest));
        System.out.println("Prediction for negative test: " + model.predict(negTest));


    }
}
