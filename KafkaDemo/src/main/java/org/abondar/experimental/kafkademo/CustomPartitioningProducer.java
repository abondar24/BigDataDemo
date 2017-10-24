package org.abondar.experimental.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class CustomPartitioningProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("partitioner.class", "org.abondar.experimental.kafkademo.DemoPartitioner");
        Producer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "testtopic";
        int messageCount = 10;

        System.out.println("Topic " + topic);
        System.out.println("Message count " + messageCount);

        Random random = new Random();
        for (int i = 0; i < messageCount; i++) {
            String clientIP = "192.168.14." + random.nextInt(255);
            String accessTime = new Date().toString();

            String msg = accessTime + ",kafka.apache.org," + clientIP;
            System.out.println(msg);
            producer.send(new ProducerRecord<>(topic, clientIP,msg));

        }

        producer.close();
    }
}
