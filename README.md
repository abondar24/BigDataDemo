# HadoopDemo
Some Hadoop Demos


# Input Data
Basic files used for input. Some demos require gz archives 
which easy to create from these files

# TODO

Run command examples for demos:


hadoop jar
10) /jarDir/MapReduceTypes-1.0-SNAPSHOT.jar org.abondar.experimental.mrtypes.PartitionByStationUsingMultipleOutputs input/190{1,2}.gz /output
13) /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.SortDataPreprocessor input/smallfiles/ input/seqs
14) /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.SortTemperatureByHashPartitioner -D mapreduce.job.reduces=30 input/seqs output-hashsort
15) /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.SortTemperatureByTotalOrderPartitioner -D mapreduce.job.reduces=30 /input/seqs output-totalsort
16) /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.MaxTemperatureBySecondarySort  /input/seqs output-ss
17) /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.JoinRecordWithStationName  /input/1901 /input/stations.txt output-ss217
18) /jarDir/MapReduceFeatures-1.0-SNAPSHOT.jar org.abondar.experimental.mrfeatures.MaxTemperatureByStationByDistCacheFile -files /input/smallfiles /input/smallfiles/  output-ss644