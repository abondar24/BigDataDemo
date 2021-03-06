# Data Integrity Demo.

Several examples of working with data before running actual Map Reduce

## Demos

1. File Decompress (fdc) - Decompress archive file using a codec.
2. Text Iterator (tic) - Iterate over characters in Text Object.
3. Sequence File Write (sfwc) - Write hardcoded data to sequence file.
4. Sequence File Read (sfrc) - Reade conents of sequence file.
5. Stream Compressor (scc) - Compress incoming data from standard input using codec.
6. Pooled Stream Compressor (pscc) - Stream compressor using compressor poool.

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
- Sequence File Write accepts output sequence file name as second argument instead of input file. File format ```some-name.seq```
- Sequence File Read accepts input sequence file name in  format ```some-name.seq```
- Demos 5-6 require as second argument classname of codec. Example codec: ```org.apache.hadoop.io.compress.GzipCodec```
