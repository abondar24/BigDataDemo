from pyspark import SparkContext
from pyspark.mlib.regression import LabeledPoint
from pyspark.mlib.feature import HashingTF
from pyspark.mlib.classification import LogisticRegressionWithSGD

import re, sys

sc = SparkContext("local", "Spam Classifier")

spam = sc.textFile(sys.argv(1))
normal = sc.textFile(sys.argv(2))

tf = HashingTF(numFeatures=10000)

spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
normalFeatures = normal.map(lambda email: tf.transform(email.split(" ")))

positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
negativeExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))
trainingData = positiveExamples.union(negativeExamples)
trainingData.cache()

model = LogisticRegressionWithSGD.train(trainingData)

posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

print "Prediction for positive test: %g" % model.predict(posTest)
print "Prediction for negative test example: %g" % model.predict(negTest)