package ru.practicum.telemetry.analyzer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Getter
@Setter
@ConfigurationProperties(prefix = "analyzer.kafka")
public class KafkaProps {
    private String bootstrapServers;
    private String snapshotsTopic;
    private String hubsTopic;
    private Integer pollTimeoutMs;
    private Map<String, String> snapshotConsumer;
    private Map<String, String> hubConsumer;
}