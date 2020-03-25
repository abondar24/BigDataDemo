package org.abondar.experimental.kafkademo.stream;


import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

public class ModelJsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper mapper;
    private Class<T> clazz;

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
