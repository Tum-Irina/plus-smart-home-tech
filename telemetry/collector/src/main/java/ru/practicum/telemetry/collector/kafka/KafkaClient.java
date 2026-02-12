package ru.practicum.telemetry.collector.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    private final KafkaProducer<String, SensorEventAvro> sensorEventProducer;
    private final KafkaProducer<String, HubEventAvro> hubEventProducer;

    @Value("${topics.sensors:telemetry.sensors.v1}")
    private String sensorsTopic;

    @Value("${topics.hubs:telemetry.hubs.v1}")
    private String hubsTopic;

    public void sendSensorEvent(SensorEventAvro event) {
        String key = event.getId();
        ProducerRecord<String, SensorEventAvro> record =
                new ProducerRecord<>(sensorsTopic, key, event);

        sensorEventProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки события датчика {} в Kafka: {}",
                        key, exception.getMessage(), exception);
            } else {
                log.debug("Событие датчика {} отправлено в топик {}, partition {}, offset {}",
                        key, metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public void sendHubEvent(HubEventAvro event) {
        String key = event.getHubId();
        ProducerRecord<String, HubEventAvro> record =
                new ProducerRecord<>(hubsTopic, key, event);

        hubEventProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки события хаба {} в Kafka: {}",
                        key, exception.getMessage(), exception);
            } else {
                log.debug("Событие хаба {} отправлено в топик {}, partition {}, offset {}",
                        key, metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public void flush() {
        sensorEventProducer.flush();
        hubEventProducer.flush();
    }
}