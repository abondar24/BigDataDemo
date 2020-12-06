package org.abondar.experimental.kafkademo.command;

import org.abondar.experimental.kafkademo.command.impl.Command;
import org.abondar.experimental.kafkademo.stream.ModelJoiner;
import org.abondar.experimental.kafkademo.stream.ModelJsonDeserializer;
import org.abondar.experimental.kafkademo.stream.ModelJsonSerializer;
import org.abondar.experimental.kafkademo.stream.ModelSerde;
import org.abondar.experimental.kafkademo.stream.StreamModel;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.time.Duration;
import java.util.Properties;

public class StreamJoinerCommand implements Command {

    @Override
    public void execute() {

        Properties props = new Properties();
        props.put("application.id", CommandUtil.APPLICATION_ID);
        props.put("bootstrap.servers", CommandUtil.KAFKA_HOST);

        Serde<String> strSerde = Serdes.String();

        ModelJsonSerializer<StreamModel> serializer = new ModelJsonSerializer<>();
        ModelJsonDeserializer<StreamModel> deserializer = new ModelJsonDeserializer<>(StreamModel.class);

        Serde<StreamModel> modelSerde = new ModelSerde(serializer, deserializer);

        KeyValueMapper<String, StreamModel, KeyValue<String, StreamModel>> kvMapper = (k, v) -> {
            StreamModel model = new StreamModel();
            model.setName(v.getName());

            return new KeyValue<>(model.getName(), model);
        };


        Predicate<String, StreamModel> namePredicate = (k, v) -> v.getName().equals("Alex");
        Predicate<String, StreamModel> idPredicate = (k, v) -> v.getId()==7;

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, StreamModel> transactionStream = builder
                .stream(CommandUtil.TEST_TOPIC, Consumed.with(strSerde, modelSerde)).map(kvMapper);
        KStream<String, StreamModel>[] branchesStream = transactionStream
                .selectKey((k, v) -> v.getName())
                .selectKey((k, v) -> String.valueOf(v.getId()))
                .branch(namePredicate,idPredicate);

        KStream<String, StreamModel> nameStream = branchesStream[0];
        KStream<String, StreamModel> idStream = branchesStream[1];


        ValueJoiner<StreamModel, StreamModel, String> joiner = new ModelJoiner();

        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        JoinWindows windows = JoinWindows.of(Duration.ofMillis(20000));

        KStream<String, String> printStream = nameStream.join(idStream, joiner, windows);
        printStream.print(Printed.<String, String>toSysOut().withLabel("Joined Stream"));


        try {
            kafkaStreams.start();
            Thread.sleep(35000);
            kafkaStreams.close();
        } catch (InterruptedException ex){
            System.err.println(ex.getMessage());
            System.exit(1);
        }
    }
}
