package ru.practicum.telemetry.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.analyzer.config.KafkaProps;
import ru.practicum.telemetry.analyzer.service.ScenarioAnalyzer;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final ScenarioAnalyzer scenarioAnalyzer;
    private final KafkaProps kafkaProps;

    public void start() {
        try {
            consumer.subscribe(java.util.List.of(kafkaProps.getSnapshotsTopic()));
            log.info("SnapshotProcessor подписался на топик {}", kafkaProps.getSnapshotsTopic());

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        consumer.poll(Duration.ofMillis(kafkaProps.getPollTimeoutMs()));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro snapshot = record.value();
                    log.debug("Получен снапшот для хаба {}", snapshot.getHubId());

                    scenarioAnalyzer.analyze(snapshot);
                }

                consumer.commitAsync();
            }
        } catch (WakeupException ignored) {
            log.info("SnapshotProcessor получил сигнал на завершение");
        } catch (Exception e) {
            log.error("Ошибка в SnapshotProcessor", e);
        } finally {
            consumer.commitSync();
            consumer.close();
            log.info("SnapshotProcessor закрыт");
        }
    }
}