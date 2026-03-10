package ru.practicum.telemetry.aggregator.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator.kafka")
public class KafkaProps {
    private String bootstrapServers;
    private String sensorsTopic;
    private String snapshotsTopic;
    private Integer pollTimeoutMs;
    private Map<String, String> consumer;
    private Map<String, String> producer;
}