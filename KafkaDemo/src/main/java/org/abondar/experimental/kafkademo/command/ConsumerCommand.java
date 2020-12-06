package org.abondar.experimental.kafkademo.command;


import org.abondar.experimental.kafkademo.command.impl.Command;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerCommand implements Command {

    @Override
    public void execute() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", CommandUtil.KAFKA_HOST);
        properties.put("zookeeper.connect",CommandUtil.ZOOKEEPER_HOST);
        properties.put("group.id",CommandUtil.TEST_GROUP);
        properties.put("zookeeper.sync.time.ms",CommandUtil.ZOOKEEPER_SYNC_TIME);
        properties.put("zookeeper.session.timeout.ms",CommandUtil.ZOOKEEPER_TIMEOUT);
        properties.put("autocommit.interval.ms",CommandUtil.ZOOKEEPER_INTERVAL);
        properties.put("key.deserializer", CommandUtil.STRING_SERIALIZER);
        properties.put("value.deserializer", CommandUtil.STRING_SERIALIZER);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(CommandUtil.TEST_TOPIC));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.println("Message: "+record.value()+" from topic: "+record.topic()+" "+" with offset "+record.offset());
            }
        }


    }
}
