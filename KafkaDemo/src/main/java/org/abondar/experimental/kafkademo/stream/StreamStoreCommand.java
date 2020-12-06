package org.abondar.experimental.kafkademo.stream;

import org.abondar.experimental.kafkademo.command.CommandUtil;
import org.abondar.experimental.kafkademo.command.impl.Command;
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

public class StreamStoreCommand implements Command {

    @Override
    public void execute() {

        Properties props = new Properties();
        props.put("application.id", CommandUtil.APPLICATION_ID);
        props.put("bootstrap.servers", CommandUtil.KAFKA_HOST);

        Serde<String> strSerde = Serdes.String();

        ModelJsonSerializer<StreamModel> serializer = new ModelJsonSerializer<>();
        ModelJsonDeserializer<StreamModel> deserializer = new ModelJsonDeserializer<>(StreamModel.class);

        Serde<StreamModel> modelSerde = new ModelSerde(serializer, deserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, StreamModel> modelKStream = builder.stream(CommandUtil.TEST_TOPIC, Consumed.with(strSerde, modelSerde));

        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(CommandUtil.TEST_STORE);
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores
                .keyValueStoreBuilder(storeSupplier, strSerde, Serdes.Long());

        builder.addStateStore(storeBuilder);


        try{
            kafkaStreams.start();
            Thread.sleep(65000);
            kafkaStreams.close();
        } catch (InterruptedException ex){
            System.err.println(ex.getMessage());
            System.exit(1);
        }

    }
}
