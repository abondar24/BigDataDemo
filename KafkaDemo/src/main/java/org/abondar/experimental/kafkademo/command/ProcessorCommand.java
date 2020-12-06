package org.abondar.experimental.kafkademo.command;

import org.abondar.experimental.kafkademo.command.CommandUtil;
import org.abondar.experimental.kafkademo.command.impl.Command;
import org.abondar.experimental.kafkademo.stream.ModelJsonDeserializer;
import org.abondar.experimental.kafkademo.stream.ModelJsonSerializer;
import org.abondar.experimental.kafkademo.stream.StreamModel;
import org.abondar.experimental.kafkademo.stream.StreamModelProcessor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class ProcessorCommand implements Command {

    @Override
    public void execute() {

        Properties props = new Properties();
        props.put("application.id", CommandUtil.APPLICATION_ID);
        props.put("bootstrap.servers", CommandUtil.KAFKA_HOST);

        Deserializer<StreamModel> modelDeserializer = new ModelJsonDeserializer<>(StreamModel.class);
        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Serializer<StreamModel> modelSerializer = new ModelJsonSerializer<>();

        Topology topology = new Topology();


        StreamModelProcessor streamModelProcessor = new StreamModelProcessor(CommandUtil.TEST_NODE);

        topology.addSource(Topology.AutoOffsetReset.LATEST,
                CommandUtil.TEST_NODE,stringDeserializer,modelDeserializer, CommandUtil.TEST_TOPIC)
                .addProcessor(CommandUtil.TEST_PROCESSOR,() -> streamModelProcessor,CommandUtil.TEST_NODE)
                .addSink(CommandUtil.TEST_SINK,CommandUtil.TEST_TOPIC,stringSerializer,modelSerializer,CommandUtil.TEST_PROCESSOR);

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
