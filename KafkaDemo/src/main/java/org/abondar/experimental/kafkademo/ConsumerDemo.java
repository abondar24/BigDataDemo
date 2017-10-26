package org.abondar.experimental.kafkademo;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","demogroup");
        properties.put("zookeeper.sync.time.ms","250");
        properties.put("zookeeper.session.timeout.ms","500");
        properties.put("autocommit.interval.ms","1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("testtopic"));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.println("Message: "+record.value()+" from topic: "+record.topic()+" "+" with offset "+record.offset());
            }
        }


    }
}
