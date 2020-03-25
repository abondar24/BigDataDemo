package org.abondar.experimental.kafkademo.stream;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class ModelJoiner  implements ValueJoiner<StreamModel,StreamModel,String> {
    @Override
    public String apply(StreamModel streamModel, StreamModel streamModel2) {
        return streamModel.getName() + ":"+streamModel2.getName();
    }
}
