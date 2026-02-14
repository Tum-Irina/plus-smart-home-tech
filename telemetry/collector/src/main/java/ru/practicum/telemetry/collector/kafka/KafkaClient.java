package ru.practicum.telemetry.collector.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClient {

    private final KafkaProducer<String, SpecificRecordBase> kafkaProducer;

    @Value("${topics.sensors:telemetry.sensors.v1}")
    private String sensorsTopic;

    @Value("${topics.hubs:telemetry.hubs.v1}")
    private String hubsTopic;

    public void sendSensorEvent(SensorEventAvro event) {
        String key = event.getId();
        long timestampMs = event.getTimestamp().toEpochMilli();
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(sensorsTopic, null, timestampMs, key, event);

        send(record);
    }

    public void sendHubEvent(HubEventAvro event) {
        String key = event.getHubId();
        long timestampMs = event.getTimestamp().toEpochMilli();
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(hubsTopic, null, timestampMs, key, event);
        send(record);
    }

    private void send(ProducerRecord<String, SpecificRecordBase> record) {
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки в Kafka: {}", exception.getMessage(), exception);
            } else {
                log.debug("Отправлено в топик {}, partition {}, offset {}, timestamp {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), record.timestamp());
            }
        });
    }

    public void flush() {
        kafkaProducer.flush();
    }
}