# MapReduce Demo

Set of map reduces demos. Demos work temperature data from National Climatic Data Center(NCDC).


## Demos

1. Basic Mapreduce (bmr)- Basic mapreduce finding maximum temperature.


## Build and Run
```
mvn clean install

cd <hadoop-dir>/bin

./hadoop jar <path-to-jar>/mapreduce.jar <demo-name>  <path-to-input-file>  <out-directory>
```

## Notes

- Demo data is in Input data directory
- Argument with demo name is in ().
