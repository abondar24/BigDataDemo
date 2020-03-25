package org.abondar.experimental.kafkademo.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class KTableDemo {
    public static void main(String[] args) throws Exception{
        String topic = "testtopic";
        String tabletopic = "testTableTopic";

        Properties props = new Properties();
        props.put("application.id", "stream-app");
        props.put("bootstrap.servers", "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String,StreamModel> table = builder.table(tabletopic);
        KStream<String,StreamModel> stream = builder.stream(topic);

        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        table.toStream().print(Printed.<String,StreamModel>toSysOut().withLabel("Table"));
        stream.print(Printed.<String,StreamModel>toSysOut().withLabel("Stream"));

        kafkaStreams.start();
        Thread.sleep(35000);
        kafkaStreams.close();
    }
}
