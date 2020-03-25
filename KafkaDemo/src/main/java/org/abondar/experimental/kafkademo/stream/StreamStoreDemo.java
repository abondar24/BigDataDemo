package org.abondar.experimental.kafkademo.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class StreamStoreDemo {

    public static void main(String[] args) throws Exception {
        String topic = "testtopic";

        Properties props = new Properties();
        props.put("application.id", "stream-app");
        props.put("bootstrap.servers", "localhost:9092");

        Serde<String> strSerde = Serdes.String();

        ModelJsonSerializer<StreamModel> serializer = new ModelJsonSerializer<>();
        ModelJsonDeserializer<StreamModel> deserializer = new ModelJsonDeserializer<>(StreamModel.class);

        Serde<StreamModel> modelSerde = new ModelSerde(serializer, deserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, StreamModel> modelKStream = builder.stream(topic, Consumed.with(strSerde, modelSerde));

        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("teststore");
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores
                .keyValueStoreBuilder(storeSupplier, strSerde, Serdes.Long());

        builder.addStateStore(storeBuilder);


        kafkaStreams.start();
        Thread.sleep(65000);
        kafkaStreams.close();
    }
}
