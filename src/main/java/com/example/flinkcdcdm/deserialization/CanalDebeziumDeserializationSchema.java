package com.example.flinkcdcdm.deserialization;

import com.example.flinkcdcdm.serializer.CanalJsonSerializer;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Flink CDC Debezium 反序列化 Schema，将 Debezium SourceRecord 转换为 Canal JSON 字符串.
 */
public class CanalDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    private transient CanalJsonSerializer serializer;

    private CanalJsonSerializer getSerializer() {
        if (serializer == null) {
            serializer = new CanalJsonSerializer();
        }
        return serializer;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        String json = getSerializer().serialize(record);
        if (json != null) {
            out.collect(json);
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
