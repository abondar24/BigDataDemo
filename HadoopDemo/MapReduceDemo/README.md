# MapReduce Demo

Set of map reduces demos. Demos work temperature data from National Climatic Data Center(NCDC).


## Demos

1. Map Reduce (mrc) - Basic map reduce finding maximum temperature.
2. Map Reduce Combiner (mrcc) - Map reduce with data combiner.


## Build and Run
```
mvn clean install

cd <hadoop-dir>/bin

./hadoop jar <path-to-jar>/mapreduce.jar <demo-name>  <path-to-input-file>  <out-directory>
```

## Notes

- Demo data is in Input data directory
- Argument with demo name is in ().
