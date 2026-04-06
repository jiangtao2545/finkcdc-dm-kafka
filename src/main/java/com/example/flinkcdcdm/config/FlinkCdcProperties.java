package com.example.flinkcdcdm.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink CDC 配置项，绑定 application.yml 中的 flink-cdc 前缀.
 */
@Component
@ConfigurationProperties(prefix = "flink-cdc")
public class FlinkCdcProperties {

    private DmProperties dm = new DmProperties();
    private KafkaProperties kafka = new KafkaProperties();
    private FlinkProperties flink = new FlinkProperties();

    public DmProperties getDm() { return dm; }
    public void setDm(DmProperties dm) { this.dm = dm; }

    public KafkaProperties getKafka() { return kafka; }
    public void setKafka(KafkaProperties kafka) { this.kafka = kafka; }

    public FlinkProperties getFlink() { return flink; }
    public void setFlink(FlinkProperties flink) { this.flink = flink; }

    /** 达梦数据库连接配置 */
    public static class DmProperties {
        private String host = "localhost";
        private int port = 5236;
        private String username = "SYSDBA";
        private String password = "SYSDBA001";
        /** Debezium logical server name，需在同一环境中唯一 */
        private String serverName = "dm_server";
        /** 要监听的 schema（database） */
        private String schemaName = "TESTDB";
        /** 监听的表列表，格式: schema.table，支持正则 */
        private List<String> tableList = new ArrayList<>();

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getServerName() { return serverName; }
        public void setServerName(String serverName) { this.serverName = serverName; }
        public String getSchemaName() { return schemaName; }
        public void setSchemaName(String schemaName) { this.schemaName = schemaName; }
        public List<String> getTableList() { return tableList; }
        public void setTableList(List<String> tableList) { this.tableList = tableList; }
    }

    /** Kafka 配置 */
    public static class KafkaProperties {
        private String bootstrapServers = "localhost:9092";
        private String topic = "dm_cdc_events";
        /** exactly-once 或 at-least-once */
        private String semantic = "at-least-once";

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getSemantic() { return semantic; }
        public void setSemantic(String semantic) { this.semantic = semantic; }
    }

    /** Flink 运行参数 */
    public static class FlinkProperties {
        private int parallelism = 1;
        private String checkpointDir = "file:///tmp/flink-checkpoints";
        private long checkpointIntervalMs = 60000L;
        /** rocksdb 或 filesystem */
        private String stateBackend = "filesystem";

        public int getParallelism() { return parallelism; }
        public void setParallelism(int parallelism) { this.parallelism = parallelism; }
        public String getCheckpointDir() { return checkpointDir; }
        public void setCheckpointDir(String checkpointDir) { this.checkpointDir = checkpointDir; }
        public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
        public void setCheckpointIntervalMs(long checkpointIntervalMs) { this.checkpointIntervalMs = checkpointIntervalMs; }
        public String getStateBackend() { return stateBackend; }
        public void setStateBackend(String stateBackend) { this.stateBackend = stateBackend; }
    }
}
