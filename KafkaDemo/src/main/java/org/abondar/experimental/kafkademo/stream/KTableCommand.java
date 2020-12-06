package org.abondar.experimental.kafkademo.stream;

import org.abondar.experimental.kafkademo.command.CommandUtil;
import org.abondar.experimental.kafkademo.command.impl.Command;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class KTableCommand implements Command {

    @Override
    public void execute() {
        Properties props = new Properties();
        props.put("application.id", "stream-app");
        props.put("bootstrap.servers", CommandUtil.KAFKA_HOST);

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String,StreamModel> table = builder.table(CommandUtil.TABLE_TOPIC);
        KStream<String,StreamModel> stream = builder.stream(CommandUtil.TEST_TOPIC);

        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        table.toStream().print(Printed.<String,StreamModel>toSysOut().withLabel("Table"));
        stream.print(Printed.<String,StreamModel>toSysOut().withLabel("Stream"));

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
