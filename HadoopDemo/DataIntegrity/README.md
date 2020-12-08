# Data Integrity Demo.

Several examples of working with data before running actual Map Reduce

## Demos

1. File Decompress (fdc) - Decompress archive file using a codec.
2. Text Iterator (tic) - Iterate over characters in Text Object.

## Build and Run
```
mvn clean install

cd <hadoop-dir>/bin

./hadoop jar <path-to-jar>/integrity.jar <demo-name>  <path-to-input-file> 
```

## Notes

- Argument with demo name is in ().
- Demo data is in Input data directory.
- File decompress requires a .gz archive
- Text iterator doesn't require any input file.
