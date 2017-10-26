# HadoopDemo
Some Hadoop,Zookeeper and Kafka Demos


# Input Data
Basic files used for input. Some demos require gz archives 
which easy to create from these files

# TODO

Run command examples for demos:

hadoop /jarDir/MapReduceTypes-1.0-SNAPSHOT.jar org.abondar.experimental.mrtypes.PartitionByStationUsingMultipleOutputs input/190{1,2}.gz /output
- /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.SortDataPreprocessor input/smallfiles/ input/seqs
- /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.SortTemperatureByHashPartitioner -D mapreduce.job.reduces=30 input/seqs output-hashsort
- /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.SortTemperatureByTotalOrderPartitioner -D mapreduce.job.reduces=30 /input/seqs output-totalsort
- /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.MaxTemperatureBySecondarySort  /input/seqs output-ss
- /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.JoinRecordWithStationName  /input/1901 /input/stations.txt output-ss217
- /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.MaxTemperatureByStationByDistCacheFile -files /input/smallfiles /input/smallfiles/  output-ss644