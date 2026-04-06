package com.example.flinkcdcdm.serializer;

import com.example.flinkcdcdm.model.CanalMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * 将 Debezium SourceRecord 转换为 Canal 风格 JSON 字符串.
 *
 * <p>Canal 格式参考: https://github.com/alibaba/canal/wiki/Canal-Protocol
 *
 * <p>与 Canal 100% 不一致的点：
 * <ul>
 *   <li>isDdl 固定为 false（DM Debezium connector 当前不支持 DDL 捕获，降级策略：忽略 DDL 事件）</li>
 *   <li>pkNames 通过 Debezium key schema 推断，而非 Canal 直接从数据库元数据获取</li>
 *   <li>es 取自 Debezium source.ts_ms（毫秒），与 Canal 的 es 字段语义对齐</li>
 *   <li>TRUNCATE/REPLACE 等操作 Canal 支持而 Debezium 可能不完全支持</li>
 * </ul>
 */
public class CanalJsonSerializer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(CanalJsonSerializer.class);

    private final ObjectMapper objectMapper;

    public CanalJsonSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    public CanalJsonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 将 Debezium SourceRecord 转换为 Canal JSON 字符串.
     *
     * @param record Debezium SourceRecord
     * @return Canal 格式 JSON 字符串，若不是 DML 事件（如 schema change）则返回 null
     */
    public String serialize(SourceRecord record) {
        if (record.value() == null) {
            return null;
        }
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();

        // 获取操作类型
        Envelope.Operation operation = getOperation(value, valueSchema);
        if (operation == null) {
            return null;
        }

        CanalMessage msg = new CanalMessage();

        // 设置 database/table
        Struct source = getSource(value, valueSchema);
        if (source != null) {
            msg.setDatabase(getStringField(source, "schema", getStringField(source, "db", "")));
            msg.setTable(getStringField(source, "table", ""));
            Object tsMs = getFieldValue(source, "ts_ms");
            if (tsMs instanceof Long) {
                msg.setEs((Long) tsMs);
            }
        }

        // 设置时间戳
        msg.setTs(System.currentTimeMillis());

        // 推断主键列名
        msg.setPkNames(extractPkNames(record));

        // 根据操作类型填充 data / old
        switch (operation) {
            case CREATE:
                msg.setType("INSERT");
                msg.setData(Collections.singletonList(structToMap(getAfter(value, valueSchema))));
                break;
            case UPDATE:
                msg.setType("UPDATE");
                msg.setData(Collections.singletonList(structToMap(getAfter(value, valueSchema))));
                Struct before = getBefore(value, valueSchema);
                if (before != null) {
                    Map<String, Object> afterMap = structToMap(getAfter(value, valueSchema));
                    Map<String, Object> beforeMap = structToMap(before);
                    // Canal 的 old 只包含变更字段
                    Map<String, Object> oldFields = new LinkedHashMap<>();
                    for (Map.Entry<String, Object> entry : beforeMap.entrySet()) {
                        Object afterVal = afterMap.get(entry.getKey());
                        if (!Objects.equals(entry.getValue(), afterVal)) {
                            oldFields.put(entry.getKey(), entry.getValue());
                        }
                    }
                    if (!oldFields.isEmpty()) {
                        msg.setOld(Collections.singletonList(oldFields));
                    }
                }
                break;
            case DELETE:
                msg.setType("DELETE");
                msg.setData(Collections.singletonList(structToMap(getBefore(value, valueSchema))));
                break;
            default:
                return null;
        }

        msg.setDdl(false);
        msg.setSql("");

        try {
            return objectMapper.writeValueAsString(msg);
        } catch (Exception e) {
            log.error("Failed to serialize CanalMessage to JSON", e);
            return null;
        }
    }

    private Envelope.Operation getOperation(Struct value, Schema valueSchema) {
        try {
            Field opField = valueSchema.field(Envelope.FieldName.OPERATION);
            if (opField == null) {
                return null;
            }
            String op = value.getString(Envelope.FieldName.OPERATION);
            return Envelope.Operation.forCode(op);
        } catch (Exception e) {
            return null;
        }
    }

    private Struct getSource(Struct value, Schema valueSchema) {
        try {
            Field sourceField = valueSchema.field(Envelope.FieldName.SOURCE);
            if (sourceField == null) return null;
            return (Struct) value.get(Envelope.FieldName.SOURCE);
        } catch (Exception e) {
            return null;
        }
    }

    private Struct getAfter(Struct value, Schema valueSchema) {
        try {
            Field afterField = valueSchema.field(Envelope.FieldName.AFTER);
            if (afterField == null) return null;
            return (Struct) value.get(Envelope.FieldName.AFTER);
        } catch (Exception e) {
            return null;
        }
    }

    private Struct getBefore(Struct value, Schema valueSchema) {
        try {
            Field beforeField = valueSchema.field(Envelope.FieldName.BEFORE);
            if (beforeField == null) return null;
            return (Struct) value.get(Envelope.FieldName.BEFORE);
        } catch (Exception e) {
            return null;
        }
    }

    private List<String> extractPkNames(SourceRecord record) {
        List<String> pkNames = new ArrayList<>();
        try {
            if (record.keySchema() != null && record.keySchema().fields() != null) {
                for (Field field : record.keySchema().fields()) {
                    pkNames.add(field.name());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to extract pk names", e);
        }
        return pkNames.isEmpty() ? null : pkNames;
    }

    private Map<String, Object> structToMap(Struct struct) {
        if (struct == null) return Collections.emptyMap();
        Map<String, Object> map = new LinkedHashMap<>();
        for (Field field : struct.schema().fields()) {
            Object val = struct.get(field);
            map.put(field.name(), val);
        }
        return map;
    }

    private String getStringField(Struct struct, String fieldName, String defaultValue) {
        try {
            Field f = struct.schema().field(fieldName);
            if (f == null) return defaultValue;
            Object val = struct.get(fieldName);
            return val != null ? val.toString() : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private Object getFieldValue(Struct struct, String fieldName) {
        try {
            Field f = struct.schema().field(fieldName);
            if (f == null) return null;
            return struct.get(fieldName);
        } catch (Exception e) {
            return null;
        }
    }
}
