package org.abondar.experimental.kafkademo.stream;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;


public class ModelJsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper mapper;
    private final Class<T> clazz;

    public ModelJsonDeserializer( Class<T> clazz){
        mapper = new ObjectMapper();
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {

        try {
            String json = new String(bytes);

            return mapper.readValue(json, clazz);
        } catch (Exception ex){
            System.out.println(ex.getMessage());
        }

        return null;
    }
}
