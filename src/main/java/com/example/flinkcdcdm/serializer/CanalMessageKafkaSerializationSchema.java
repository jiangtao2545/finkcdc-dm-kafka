package com.example.flinkcdcdm.serializer;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * 将 Canal JSON 字符串序列化为 Kafka ProducerRecord.
 */
public class CanalMessageKafkaSerializationSchema
        implements KafkaRecordSerializationSchema<String> {

    private final String topic;

    public CanalMessageKafkaSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            String element,
            KafkaSinkContext context,
            Long timestamp) {
        if (element == null) {
            return null;
        }
        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
    }
}
