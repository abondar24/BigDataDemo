package org.abondar.experimental.kafkademo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadedConsumerDemo {

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

        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<KafkaConsumerRunner> runners = new ArrayList<>();
        for (int i=0;i<5;i++){
            KafkaConsumerRunner runner = new KafkaConsumerRunner(properties);
            executor.submit(runner);
            runners.add(runner);
        }

        try	{
            Thread.sleep(10000);
        }	catch	(InterruptedException	ie)	{
        }

       for (KafkaConsumerRunner runner:runners){
            runner.shutdown();
       }
       executor.shutdown();
    }
}
