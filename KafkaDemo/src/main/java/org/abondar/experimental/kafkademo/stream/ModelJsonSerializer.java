package org.abondar.experimental.kafkademo.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;


public class ModelJsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper mapper;

    public ModelJsonSerializer(){
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String s, T t) {
        try {
            String json = mapper.writeValueAsString(t);
            return json.getBytes();
        } catch (Exception e){
            System.out.println(e.getMessage());
        }

       return new byte[0];
    }

}
