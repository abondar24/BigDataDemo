# Kafka demo

Kafka basics

## Demos

1. Producer (prc) - Dummy Kafka producer.
2. Consumer (coc) - Dummy Kafka consumer.
3. Custom Partitioning Producer (cpc) - Producer with partitioning.
4. Multi Threaded Consumer (mtc) - Advanced Kafka consumer.
5. K-Table (ktc) - Kafka streams with K-Tables.
6. Processor (poc) - Basic Kafka processor.
7. Stream Processor (spc) - Basic Kafka stream processor.
8. Stream Joiner (sjc) - Basic Kafka stream joiner.
9. Stream Store (ssc) - Basic Kafka stream store.

## Build and run

```
mvn clean install

java -jar <jar-location>/kafka.jar <arg>
```

## Notes

-  Argumets are in ()
-  Run consumers and producers in parallel
