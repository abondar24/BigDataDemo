package org.abondar.experimental.kafkademo.command;

import org.abondar.experimental.kafkademo.command.impl.Command;
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

public class StreamProcessorCommand implements Command {


    @Override
    public void execute() {
        Properties props = new Properties();
        props.put("application.id", CommandUtil.APPLICATION_ID);
        props.put("bootstrap.servers", CommandUtil.KAFKA_HOST);

        Serde<String> serde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream1 = builder.stream(CommandUtil.TEST_TOPIC, Consumed.with(serde, serde));

        KStream<String, String> stream2 = stream1.mapValues((ValueMapper<String, String>) String::toLowerCase);

        stream2.to(CommandUtil.STREAM_TOPIC, Produced.with(serde, serde));

        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

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
