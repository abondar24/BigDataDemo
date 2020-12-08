#HDFS Demo.

Several examples of Hadoop Distributes File System usage.

## Demos

1. Copy File (cfc) - Copy a file from local file system to hdfs

## Build and Run
```
mvn clean install

cd <hadoop-dir>/bin

./hadoop jar <path-to-jar>/hdfs.jar <demo-name>  <path-to-input-file> 
```

## Notes

- Argument with demo name is in ().
- Demo data is in Input data directory.
- Copy File requires and additional argument - directory name on hadoop fs. Format hdfs://localhost/<user>/<path>/<file-name>
