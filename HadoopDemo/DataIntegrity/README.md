# Data Integrity Demo.

Several examples of working with data before running actual Map Reduce

## Demos

1. 
## Build and Run
```
mvn clean install

cd <hadoop-dir>/bin

./hadoop jar <path-to-jar>/integrity.jar <demo-name>  <path-to-input-file> 
```

## Notes

- Argument with demo name is in ().
- Demo data is in Input data directory.
- Copy File requires and additional argument - directory name on hadoop fs. Format hdfs://localhost/<user>/<path>/<file-name>
- Input files for demo 2-5 must be on hdfs. Copy them using Copy File demo.
- List status requires path to HDFS directory instead of file.
