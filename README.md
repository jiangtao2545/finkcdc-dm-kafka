# finkcdc-dm-kafka

基于 **Spring Boot 2 + Flink CDC + Debezium** 实现**达梦（DM8）数据库** CDC 增量捕获，并将变更事件以 **Canal 风格 JSON** 写入 **Kafka**。

## 技术栈

| 组件 | 版本 |
|------|------|
| JDK | 8 |
| Spring Boot | 2.7.18 |
| Apache Flink | 1.16.3 |
| Flink CDC (Ververica) | 2.3.0 |
| Debezium | 1.9.7.Final |
| Kafka Client | 3.3.x |

---

## 快速开始

### 1. 前置条件

- JDK 8+
- Maven 3.6+
- 运行中的 DM8 实例（开启逻辑日志）
- 运行中的 Kafka

### 2. 安装 DM Debezium Connector

DM Debezium Connector 未发布到 Maven Central，需手动安装到本地 Maven 仓库：

```bash
# 从达梦官方或第三方渠道获取 debezium-connector-dm.jar
mvn install:install-file \
  -Dfile=debezium-connector-dm-1.9.7.Final.jar \
  -DgroupId=io.debezium \
  -DartifactId=debezium-connector-dm \
  -Dversion=1.9.7.Final \
  -Dpackaging=jar
```

### 3. 配置

编辑 `src/main/resources/application.yml`：

```yaml
flink-cdc:
  dm:
    host: 192.168.1.100          # DM 主机
    port: 5236                   # DM 端口
    username: SYSDBA             # 用户名
    password: SYSDBA001          # 密码
    schema-name: TESTDB          # Schema 名
    table-list:                  # 监听的表（格式: schema.table）
      - TESTDB.ORDERS
      - TESTDB.CUSTOMERS
  kafka:
    bootstrap-servers: localhost:9092
    topic: dm_cdc_events
```

### 4. 编译与打包

```bash
mvn clean package -DskipTests
```

### 5. 运行测试

```bash
mvn test
```

### 6. 启动应用

```bash
java -jar target/finkcdc-dm-kafka-1.0.0-SNAPSHOT.jar
```

### 7. 验证 Kafka 消息

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dm_cdc_events \
  --from-beginning \
  --property print.value=true
```

**INSERT 示例输出**：
```json
{
  "database": "TESTDB",
  "table": "ORDERS",
  "type": "INSERT",
  "es": 1700000000000,
  "ts": 1700000000123,
  "pkNames": ["id"],
  "isDdl": false,
  "sql": "",
  "data": [{"id": 1, "name": "Alice", "age": 30}]
}
```

**UPDATE 示例输出**：
```json
{
  "database": "TESTDB",
  "table": "ORDERS",
  "type": "UPDATE",
  "es": 1700000001000,
  "ts": 1700000001234,
  "pkNames": ["id"],
  "isDdl": false,
  "sql": "",
  "data": [{"id": 1, "name": "Alice", "age": 31}],
  "old": [{"age": 30}]
}
```

---

## Canal 兼容性说明

本项目输出 JSON 尽可能与 Canal 的 **flat message** 格式保持一致，以下是已知差异：

| 字段 | Canal 行为 | 本项目行为 | 兼容策略 |
|------|-----------|-----------|---------|
| `isDdl` | 支持 DDL 捕获 | 固定 `false`，不支持 DDL | DM Debezium Connector 暂不稳定支持 DDL，忽略 DDL 事件 |
| `sql` | DDL 时有 SQL | 固定空字符串 | 同上 |
| `old` | UPDATE 时包含所有旧字段 | 只包含**变更字段** | 更节省带宽；如需完整旧行，可修改 `CanalJsonSerializer` |
| `pkNames` | 从数据库元数据获取 | 从 Debezium Key Schema 推断 | 语义等价，通常一致 |
| `TRUNCATE` | 支持 | 不支持（Debezium 不产生对应事件） | 降级：忽略该操作 |
| `es` | 事件时间（ms） | Debezium `source.ts_ms` | 语义对齐 |

---

## 工程结构

```
src/
├── main/
│   ├── java/com/example/flinkcdcdm/
│   │   ├── FlinkCdcDmKafkaApplication.java    # Spring Boot 启动入口
│   │   ├── config/
│   │   │   └── FlinkCdcProperties.java         # 配置绑定
│   │   ├── model/
│   │   │   └── CanalMessage.java               # Canal 消息模型
│   │   ├── serializer/
│   │   │   ├── CanalJsonSerializer.java         # Canal JSON 转换（可单测）
│   │   │   └── CanalMessageKafkaSerializationSchema.java
│   │   ├── deserialization/
│   │   │   └── CanalDebeziumDeserializationSchema.java
│   │   └── job/
│   │       └── DmCdcToKafkaJob.java             # Flink Job 定义
│   └── resources/
│       └── application.yml
└── test/
    └── java/com/example/flinkcdcdm/
        └── serializer/
            └── CanalJsonSerializerTest.java      # 单元测试
```

---

## DM8 CDC 配置要求

在 DM8 数据库端需要开启逻辑日志：

```sql
-- 开启归档日志（DBA 执行）
ALTER DATABASE MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- 开启逻辑日志
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- 创建 CDC 用户并授权
CREATE USER CDC_USER IDENTIFIED BY "CDC_Pass123";
GRANT DBA TO CDC_USER;
```

详细配置请参考达梦官方 Debezium Connector 文档。
