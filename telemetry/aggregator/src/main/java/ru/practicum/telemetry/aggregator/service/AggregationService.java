package ru.practicum.telemetry.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.aggregator.config.KafkaProps;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class AggregationService {

    private final Consumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final KafkaProps kafkaProps;

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public AggregationService(
            Consumer<String, SensorEventAvro> consumer,
            KafkaProducer<String, SpecificRecordBase> producer,
            KafkaProps kafkaProps
    ) {
        this.consumer = consumer;
        this.producer = producer;
        this.kafkaProps = kafkaProps;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Сработал хук на завершение JVM. Прерываю работу консьюмера.");
            consumer.wakeup();
        }));
    }

    public void start() {
        try {
            consumer.subscribe(java.util.List.of(kafkaProps.getSensorsTopic()));
            log.info("Aggregator подписался на топик {}", kafkaProps.getSensorsTopic());

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(
                        Duration.ofMillis(kafkaProps.getPollTimeoutMs()));

                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    log.debug("Получено событие: hubId={}, sensorId={}", event.getHubId(), event.getId());

                    Optional<SensorsSnapshotAvro> updatedSnapshot = updateState(event);

                    if (updatedSnapshot.isPresent()) {
                        SensorsSnapshotAvro snapshot = updatedSnapshot.get();
                        ProducerRecord<String, SpecificRecordBase> producerRecord =
                                new ProducerRecord<>(kafkaProps.getSnapshotsTopic(), snapshot.getHubId(), snapshot);
                        producer.send(producerRecord);
                        log.info("Снапшот для хаба {} обновлён и отправлен", snapshot.getHubId());
                    }
                }

                producer.flush();
                consumer.commitAsync();
            }

        } catch (WakeupException ignored) {
            log.info("Получен сигнал на завершение работы");
        } catch (Exception e) {
            log.error("Ошибка в работе Aggregator", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                consumer.close();
                producer.close();
                log.info("Ресурсы Aggregator закрыты");
            }
        }
    }

    private Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        Instant eventTime = event.getTimestamp();

        SensorsSnapshotAvro snapshot = snapshots.get(hubId);
        if (snapshot == null) {
            snapshot = SensorsSnapshotAvro.newBuilder()
                    .setHubId(hubId)
                    .setTimestamp(eventTime)
                    .setSensorsState(new HashMap<>())
                    .build();
            snapshots.put(hubId, snapshot);
            log.debug("Создан новый снапшот для хаба {}", hubId);
        }

        SensorStateAvro oldState = snapshot.getSensorsState().get(sensorId);

        if (oldState != null) {
            if (oldState.getTimestamp().isAfter(eventTime)) {
                log.debug("Событие устарело (старый таймстемп {} >= новый {})",
                        oldState.getTimestamp(), eventTime);
                return Optional.empty();
            }

            if (oldState.getData().equals(event.getPayload())) {
                log.debug("Данные не изменились");
                return Optional.empty();
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTime)
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(sensorId, newState);
        snapshot.setTimestamp(eventTime);

        log.debug("Состояние обновлено");
        return Optional.of(snapshot);
    }
}