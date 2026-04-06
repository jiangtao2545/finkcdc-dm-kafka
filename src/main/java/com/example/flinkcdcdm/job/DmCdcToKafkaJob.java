package com.example.flinkcdcdm.job;

import com.example.flinkcdcdm.config.FlinkCdcProperties;
import com.example.flinkcdcdm.deserialization.CanalDebeziumDeserializationSchema;
import com.example.flinkcdcdm.serializer.CanalMessageKafkaSerializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * DM CDC → Kafka Flink Job.
 *
 * <p>使用 Debezium SourceFunction（支持运行时加载 DM Connector）捕获 DM 变更，
 * 转换为 Canal JSON 后写入 Kafka.
 *
 * <p><b>运行前提</b>：需将 debezium-connector-dm.jar 放在 classpath（或 Flink lib 目录）中。
 */
@Component
public class DmCdcToKafkaJob {

    private static final Logger log = LoggerFactory.getLogger(DmCdcToKafkaJob.class);

    private final FlinkCdcProperties props;

    public DmCdcToKafkaJob(FlinkCdcProperties props) {
        this.props = props;
    }

    /**
     * 构建并执行 Flink 作业.
     */
    public void execute() throws Exception {
        StreamExecutionEnvironment env = buildEnvironment();

        DebeziumSourceFunction<String> dmSource = buildDmSource();

        DataStreamSource<String> stream = env
                .addSource(dmSource, "DM-CDC-Source")
                .setParallelism(1); // CDC source 必须为 1

        KafkaSink<String> kafkaSink = buildKafkaSink();

        stream.sinkTo(kafkaSink)
              .setParallelism(props.getFlink().getParallelism());

        log.info("Starting Flink CDC job: DM -> Kafka (topic={})", props.getKafka().getTopic());
        env.execute("DM-CDC-to-Kafka");
    }

    private StreamExecutionEnvironment buildEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(props.getFlink().getParallelism());
        env.enableCheckpointing(
                props.getFlink().getCheckpointIntervalMs(),
                CheckpointingMode.AT_LEAST_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        return env;
    }

    /**
     * 构建 DM CDC Source（基于 Debezium DebeziumSourceFunction）.
     *
     * <p>运行时需要 debezium-connector-dm.jar 在 classpath 中，
     * 可从达梦官网或第三方仓库获取后放入 Flink 的 lib 目录。
     */
    @SuppressWarnings("deprecation")
    private DebeziumSourceFunction<String> buildDmSource() {
        FlinkCdcProperties.DmProperties dm = props.getDm();
        Properties debeziumProps = new Properties();

        // DM Debezium Connector（运行时加载）
        debeziumProps.setProperty("connector.class", "io.debezium.connector.dm.DmConnector");
        debeziumProps.setProperty("database.hostname", dm.getHost());
        debeziumProps.setProperty("database.port", String.valueOf(dm.getPort()));
        debeziumProps.setProperty("database.user", dm.getUsername());
        debeziumProps.setProperty("database.password", dm.getPassword());
        debeziumProps.setProperty("database.server.name", dm.getServerName());
        debeziumProps.setProperty("database.dbname", dm.getSchemaName());

        if (!dm.getTableList().isEmpty()) {
            // 格式: schema.table1,schema.table2
            debeziumProps.setProperty("table.include.list",
                    String.join(",", dm.getTableList()));
        }

        // Offset 存储（生产建议使用 Kafka 或 Redis）
        debeziumProps.setProperty("offset.storage",
                "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        debeziumProps.setProperty("offset.storage.file.filename",
                "/tmp/dm_cdc_offsets.dat");
        debeziumProps.setProperty("offset.flush.interval.ms", "10000");

        // 历史存储（DM Connector 可能需要）
        debeziumProps.setProperty("database.history",
                "io.debezium.relational.history.FileDatabaseHistory");
        debeziumProps.setProperty("database.history.file.filename",
                "/tmp/dm_cdc_history.dat");

        return new DebeziumSourceFunction<>(
                new CanalDebeziumDeserializationSchema(),
                debeziumProps,
                null,
                Validator.getDefaultValidator());
    }

    private KafkaSink<String> buildKafkaSink() {
        FlinkCdcProperties.KafkaProperties kafka = props.getKafka();

        KafkaSinkBuilder<String> builder = KafkaSink.<String>builder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setRecordSerializer(
                        new CanalMessageKafkaSerializationSchema(kafka.getTopic()));

        if ("exactly-once".equalsIgnoreCase(kafka.getSemantic())) {
            builder.setTransactionalIdPrefix("dm-cdc-");
        }

        return builder.build();
    }
}
