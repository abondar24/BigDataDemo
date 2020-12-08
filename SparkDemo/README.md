# SparkDemo
Several Apache Spark demos implemented on Java,Scala and Python

## Demos

1. Max Temperature - Find maximum temperature in dataset using map reduce.
2. Spam Classifier - Detect spam in incoming data.
3. Stream from Console (Java/Scala)- Print data from console.
4. Load Json Hive (Java/Scala) - Read data from JSON to Hive and query it.
5. Word Count - Count words in dataset.
6. Empty Line (Scala and Python) - Count number of empty lines in dataset.
7. Error Counter (Python) - Calculate number of words in dataset. not matching regular expression.


## Build and Run

### Java and Scala

```
mvn clean install 

cd <spark-dir>
spark-submit --class <full-class-name> --master local <path-to-jar>/SparkDemo-1.0.jar <input-file> <output-dir>
```

### Python
```
pip install pyspark
pip install 

spark-submit --master local <path-to-script> <input-file> <out-dir>
```

# Notes

- Input files are in input data directory.
