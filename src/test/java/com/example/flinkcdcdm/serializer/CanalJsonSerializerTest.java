package com.example.flinkcdcdm.serializer;

import com.example.flinkcdcdm.model.CanalMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CanalJsonSerializer 单元测试.
 * 不依赖任何外部服务（DM / Kafka / Flink）.
 */
class CanalJsonSerializerTest {

    private CanalJsonSerializer serializer;
    private ObjectMapper objectMapper;

    /** 行数据 Schema */
    private Schema rowSchema;
    /** Source Schema */
    private Schema sourceSchema;
    /** Key Schema（主键） */
    private Schema keySchema;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        serializer = new CanalJsonSerializer(objectMapper);

        // 行数据 Schema: id, name, age
        rowSchema = SchemaBuilder.struct().name("TESTDB.ORDERS.Value")
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        // Source Schema
        sourceSchema = SchemaBuilder.struct().name("io.debezium.connector.dm.Source")
                .field("db", Schema.STRING_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();

        // Key Schema（主键列）
        keySchema = SchemaBuilder.struct().name("TESTDB.ORDERS.Key")
                .field("id", Schema.INT64_SCHEMA)
                .build();
    }

    /** 构建 Debezium Envelope Schema */
    private Schema buildEnvelopeSchema(Schema beforeSchema, Schema afterSchema) {
        return SchemaBuilder.struct()
                .field(Envelope.FieldName.BEFORE, beforeSchema)
                .field(Envelope.FieldName.AFTER, afterSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    /** 构建 Source Struct */
    private Struct buildSource(long tsMs) {
        return new Struct(sourceSchema)
                .put("db", "TESTDB")
                .put("schema", "TESTDB")
                .put("table", "ORDERS")
                .put("ts_ms", tsMs);
    }

    // ======================== INSERT 测试 ========================

    @Test
    void testInsertEvent_shouldGenerateCanalJsonWithCorrectFields() throws Exception {
        // 构建 Envelope（INSERT: before=null, after=row data）
        Schema envelopeSchema = buildEnvelopeSchema(
                SchemaBuilder.struct().optional().build(),
                rowSchema);

        Struct after = new Struct(rowSchema)
                .put("id", 1L)
                .put("name", "Alice")
                .put("age", 30);

        Struct source = buildSource(1700000000000L);

        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.BEFORE, null)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code());

        Struct key = new Struct(keySchema).put("id", 1L);
        SourceRecord record = new SourceRecord(
                Collections.singletonMap("server", "dm_server"),
                Collections.singletonMap("pos", 0L),
                "TESTDB.ORDERS",
                keySchema, key,
                envelopeSchema, envelope);

        String json = serializer.serialize(record);

        assertNotNull(json, "JSON 不应为 null");
        CanalMessage msg = objectMapper.readValue(json, CanalMessage.class);

        // 验证基础字段
        assertEquals("TESTDB", msg.getDatabase());
        assertEquals("ORDERS", msg.getTable());
        assertEquals("INSERT", msg.getType());
        assertFalse(msg.isDdl(), "非 DDL 事件");
        assertEquals("", msg.getSql());

        // 验证 data 字段
        assertNotNull(msg.getData());
        assertEquals(1, msg.getData().size());
        Map<String, Object> row = msg.getData().get(0);
        assertEquals(1L, ((Number) row.get("id")).longValue());
        assertEquals("Alice", row.get("name"));
        assertEquals(30, ((Number) row.get("age")).intValue());

        // 验证 old 字段（INSERT 无旧值）
        assertNull(msg.getOld(), "INSERT 事件 old 应为 null");

        // 验证 pkNames
        assertNotNull(msg.getPkNames());
        assertTrue(msg.getPkNames().contains("id"));

        // 验证时间戳
        assertNotNull(msg.getEs());
        assertNotNull(msg.getTs());
    }

    // ======================== UPDATE 测试 ========================

    @Test
    void testUpdateEvent_shouldGenerateCanalJsonWithOldFields() throws Exception {
        Schema envelopeSchema = buildEnvelopeSchema(rowSchema, rowSchema);

        // UPDATE: age 从 30 改为 31，name 不变
        Struct before = new Struct(rowSchema)
                .put("id", 1L)
                .put("name", "Alice")
                .put("age", 30);
        Struct after = new Struct(rowSchema)
                .put("id", 1L)
                .put("name", "Alice")
                .put("age", 31);

        Struct source = buildSource(1700000001000L);

        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.BEFORE, before)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code());

        Struct key = new Struct(keySchema).put("id", 1L);
        SourceRecord record = new SourceRecord(
                Collections.singletonMap("server", "dm_server"),
                Collections.singletonMap("pos", 1L),
                "TESTDB.ORDERS",
                keySchema, key,
                envelopeSchema, envelope);

        String json = serializer.serialize(record);

        assertNotNull(json);
        CanalMessage msg = objectMapper.readValue(json, CanalMessage.class);

        assertEquals("UPDATE", msg.getType());
        assertEquals("TESTDB", msg.getDatabase());
        assertEquals("ORDERS", msg.getTable());

        // 验证 data（after）
        assertNotNull(msg.getData());
        assertEquals(1, msg.getData().size());
        assertEquals(31, ((Number) msg.getData().get(0).get("age")).intValue());

        // 验证 old（只包含变更字段：age 从 30 -> 31）
        assertNotNull(msg.getOld(), "UPDATE 事件 old 不应为 null");
        assertEquals(1, msg.getOld().size());
        Map<String, Object> old = msg.getOld().get(0);
        assertTrue(old.containsKey("age"), "old 应包含变更字段 age");
        assertEquals(30, ((Number) old.get("age")).intValue());
        assertFalse(old.containsKey("name"), "old 不应包含未变更字段 name");
        assertFalse(old.containsKey("id"), "old 不应包含未变更字段 id");

        // 验证 pkNames
        assertNotNull(msg.getPkNames());
        assertTrue(msg.getPkNames().contains("id"));
    }

    // ======================== DELETE 测试 ========================

    @Test
    void testDeleteEvent_shouldGenerateCanalJsonWithBeforeData() throws Exception {
        Schema envelopeSchema = buildEnvelopeSchema(rowSchema,
                SchemaBuilder.struct().optional().build());

        Struct before = new Struct(rowSchema)
                .put("id", 2L)
                .put("name", "Bob")
                .put("age", 25);

        Struct source = buildSource(1700000002000L);

        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.BEFORE, before)
                .put(Envelope.FieldName.AFTER, null)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.DELETE.code());

        Struct key = new Struct(keySchema).put("id", 2L);
        SourceRecord record = new SourceRecord(
                Collections.singletonMap("server", "dm_server"),
                Collections.singletonMap("pos", 2L),
                "TESTDB.ORDERS",
                keySchema, key,
                envelopeSchema, envelope);

        String json = serializer.serialize(record);
        assertNotNull(json);

        CanalMessage msg = objectMapper.readValue(json, CanalMessage.class);
        assertEquals("DELETE", msg.getType());
        assertNotNull(msg.getData());
        assertEquals(2L, ((Number) msg.getData().get(0).get("id")).longValue());
    }

    // ======================== 边界测试 ========================

    @Test
    void testNullValueRecord_shouldReturnNull() {
        SourceRecord record = new SourceRecord(
                Collections.singletonMap("server", "dm_server"),
                Collections.singletonMap("pos", 0L),
                "TESTDB.ORDERS",
                null, null,
                null, null);

        String json = serializer.serialize(record);
        assertNull(json, "value 为 null 时应返回 null");
    }
}
