package org.abondar.experimental.kafkademo.stream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class ProcessorDemo {

    public static void main(String[] args) throws Exception {
        String topic = "testtopic";

        Properties props = new Properties();
        props.put("application.id", "stream-app");
        props.put("bootstrap.servers", "localhost:9092");

        Deserializer<StreamModel> modelDeserializer = new ModelJsonDeserializer<>(StreamModel.class);
        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Serializer<StreamModel> modelSerializer = new ModelJsonSerializer<>();

        Topology topology = new Topology();

        String sink = "demo-sink";
        String node = "demo-node";
        String processor = "demo-processor";

        StreamModelProcessor streamModelProcessor = new StreamModelProcessor(node);

        topology.addSource(Topology.AutoOffsetReset.LATEST,
                node,stringDeserializer,modelDeserializer,topic)
                .addProcessor(processor,() -> streamModelProcessor,node)
                .addSink(sink,topic,stringSerializer,modelSerializer,processor);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        kafkaStreams.start();
        Thread.sleep(35000);
        kafkaStreams.close();
    }
}
