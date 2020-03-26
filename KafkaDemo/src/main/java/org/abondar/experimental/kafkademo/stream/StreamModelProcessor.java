package org.abondar.experimental.kafkademo.stream;


import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;

public class StreamModelProcessor extends AbstractProcessor<String, StreamModel> {


    private String node;

    public StreamModelProcessor(String node) {
        this.node = node;
    }


    @Override
    public void process(String key, StreamModel streamModel) {
        context().forward(key,streamModel.getName(), To.child(node));
    }
}
