package org.abondar.experimental.kafkademo.command;


public class CommandUtil {

    public static final String KAFKA_HOST = "localhost:9092";

    public static final String ZOOKEEPER_HOST = "localhost:2181";

    public static final String TEST_TOPIC = "test-topic";

    public static final String TEST_GROUP = "test-group";

    public static final String TABLE_TOPIC = "table-topic";

    public static final String APPLICATION_ID = "stream-app";

    public static final String TEST_SINK = "test-sink";

    public static final String TEST_NODE = "test-node";

    public static final String TEST_PROCESSOR = "test-processor";

    public static final String TEST_STORE = "test-store";

    public static final String STREAM_TOPIC = "stream-topic";

    public static final String SERIALIZER = "kafka.serializer.StringEncoder";

    public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    public static final String REQUIRED_ACKS = "1";

    public static final String PARTITIONER = "org.abondar.experimental.kafkademo.DemoPartitioner";

    public static final String ZOOKEEPER_SYNC_TIME = "250";

    public static final String ZOOKEEPER_TIMEOUT = "500";

    public static final String ZOOKEEPER_INTERVAL = "1000";

    private CommandUtil(){}
}
