package org.abondar.experimental.kafkademo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Properties;

public class StreamProcessorDemo {

    public static void main(String[] args) throws Exception {
        String topic = "testtopic";
        String streamTopic = "streamtopic";

        Properties props = new Properties();
        props.put("application.id", "stream-app");
        props.put("bootstrap.servers", "localhost:9092");

        Serde<String> serde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream1 = builder.stream(topic, Consumed.with(serde, serde));

        KStream<String, String> stream2 = stream1.mapValues((ValueMapper<String, String>) String::toLowerCase);

        stream2.to(streamTopic, Produced.with(serde, serde));

        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        kafkaStreams.start();
        Thread.sleep(35000);
        kafkaStreams.close();


    }
}
