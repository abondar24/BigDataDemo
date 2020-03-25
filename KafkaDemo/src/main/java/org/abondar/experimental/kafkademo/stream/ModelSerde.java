package org.abondar.experimental.kafkademo.stream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class ModelSerde extends Serdes.WrapperSerde<StreamModel> {
    public ModelSerde(Serializer<StreamModel> serializer, Deserializer<StreamModel> deserializer) {
        super(serializer, deserializer);
    }

}
