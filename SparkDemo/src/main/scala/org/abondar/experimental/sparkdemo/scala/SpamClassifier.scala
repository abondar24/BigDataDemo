package org.abondar.experimental.sparkdemo.scala



import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}



object SpamClassifier {
  //this example for rdd based lr. but it will be deprecated in spark 3.0
  // so we need demo for dataset based as well
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spam classifier")
    val sc = new SparkContext(conf)

    val spam = sc.textFile(args(0))
    val normal = sc.textFile(args(1))

    val tf = new HashingTF(numFeatures = 10000)

    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1,features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0,features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache()

    val lr = new LogisticRegressionWithLBFGS().run(trainingData)

    val posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    println("Prediction for positive test: " + lr.predict(posTest))
    println("Prediction for negative test: " + lr.predict(negTest))



  }
}
