package org.abondar.experimental.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerRunner implements Runnable{

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String,String> consumer;

    public KafkaConsumerRunner(Properties properties) {
        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList("testtopic"));
            while (!closed.get()){
                ConsumerRecords<String,String> records = consumer.poll(1000);
                for (ConsumerRecord<String,String> record:records){
                    System.out.println("Message: "+record.value()+" from topic: "+record.topic()+" "+" with offset "+record.offset());
                }
            }
        } catch (WakeupException e){
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    public void shutdown(){
        closed.set(true);
        consumer.wakeup();
    }
}
