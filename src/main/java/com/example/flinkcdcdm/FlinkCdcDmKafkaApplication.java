package com.example.flinkcdcdm;

import com.example.flinkcdcdm.job.DmCdcToKafkaJob;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot 启动入口.
 * 启动后通过 CommandLineRunner 触发 Flink CDC Job.
 */
@SpringBootApplication
public class FlinkCdcDmKafkaApplication implements CommandLineRunner {

    private final DmCdcToKafkaJob dmCdcToKafkaJob;

    public FlinkCdcDmKafkaApplication(DmCdcToKafkaJob dmCdcToKafkaJob) {
        this.dmCdcToKafkaJob = dmCdcToKafkaJob;
    }

    public static void main(String[] args) {
        SpringApplication.run(FlinkCdcDmKafkaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        dmCdcToKafkaJob.execute();
    }
}
