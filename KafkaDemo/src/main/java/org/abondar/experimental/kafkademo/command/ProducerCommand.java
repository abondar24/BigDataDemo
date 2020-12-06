package org.abondar.experimental.kafkademo.command;



import org.abondar.experimental.kafkademo.command.impl.Command;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class ProducerCommand implements Command {


    @Override
    public void execute() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", CommandUtil.KAFKA_HOST);
        properties.put("serializer.class",CommandUtil.SERIALIZER);
        properties.put("request.required.acks",CommandUtil.REQUIRED_ACKS);
        properties.put("key.serializer", CommandUtil.STRING_SERIALIZER);
        properties.put("value.serializer", CommandUtil.STRING_SERIALIZER);

        Producer<String,String> producer = new KafkaProducer<>(properties);

        int messageCount=10;

        System.out.println("Topic "+CommandUtil.TEST_TOPIC);
        System.out.println("Message count "+messageCount);

        for (int i=0;i<messageCount;i++){
            String runtime = new Date().toString();

            String msg = "Message publishing time: "+runtime;
            System.out.println(msg);
            producer.send(new ProducerRecord<>(CommandUtil.TEST_TOPIC, msg));

        }

        producer.close();
    }
}
