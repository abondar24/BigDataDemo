# MapReduce Demo

Set of map reduces demos. Demos work temperature data from National Climatic Data Center(NCDC).


## Demos

1. Configuration Printer (cpc) - Print hadoop configuration info.
2. Map Reduce (mrc) - Basic map reduce finding maximum temperature.
3. Map Reduce Combiner (mrcc) - Map reduce with data combiner.
4. Map Reduce Driver (mrdc) - Basic map reduce adopted to driver usage.
5. Map Reduce Mimimal (mrmc) - Map reduce without custom mapper and reducer.
6. Map Reduce Compressor (mrco) - Map reduce with data compressor.
7. Map Reduce Counter (mrcn) - Map reduce including counting missing and malformed fields and quality codes.
8. Missing Temperature Fields (mtfc) - Calculate the proportion of records with missing temperature fields.
9. Sort Data Preprocessor (sdpc) - Transform the weather data into Sequence File format.
10. Sequence File Converter (sfcc) - Package a collection of small files as a single SequenceFile
11. Sort Temperature Hash Partitioner (sthp) - Sort a SequenceFile with IntWritable keys using the default HashPartitioner.
12. Sort Temperature Total Order Partitioner (stto) - Sort a SequenceFile with IntWritable keys using the TotalOrderPartitioner to globally sort the data
13. Map Reduce Secondary Sort (mrss) - Map Reduce with sorting temperatures in the key sorting temperatures in the key.

## Build and Run
```
mvn clean install

cd <hadoop-dir>/bin

./hadoop jar <path-to-jar>/mapreduce.jar <demo-name>  <path-to-input-file>  <out-directory>
```

## Notes

- Demo data is in Input data directory
- Argument with demo name is in ().
- Configuration printer requires no input files.
- Use different out directories for demos or remove  after previous run.
- Missing Temperature Fields requires job id argument(format job_1410450250506_0007) instead of input data. Any map reduce job must be run before this demo
- Sort Temperature Hash Partitioner requires sequence file which is an output of Sort Data Preprocessor or Sequence File Converter.
- Sort Temperature Total Order Partitioner requires sequence file which is an output of Sort Data Preprocessor or Sequence File Converter.
