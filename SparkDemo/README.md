# SparkDemo
Apache Spark demos


Run instruction

1) spark-submit --class org.abondar.experimental.sparkdemo.scala.MaxTemp --master local SparkDemo-1.0-SNAPSHOT.jar sample.txt /output
2) spark-submit --class org.abondar.experimental.sparkdemo.java.MaxTemp --master local SparkDemo-1.0-SNAPSHOT.jar sample.txt /output
3) spark-submit --master local maxtemp.py sample.txt /output