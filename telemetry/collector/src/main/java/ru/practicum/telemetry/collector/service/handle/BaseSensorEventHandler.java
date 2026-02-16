package ru.practicum.telemetry.collector.service.handle;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.beans.factory.annotation.Value;
import ru.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {

    private final KafkaClient kafkaClient;

    @Value("${topics.sensors}")
    private String sensorsTopic;

    @Override
    public void handle(SensorEvent event) {
        if (!event.getType().equals(getMessageType())) {
            throw new IllegalArgumentException(
                    String.format("Неизвестный тип события: %s. Ожидался: %s",
                            event.getType(), getMessageType())
            );
        }

        T payload = mapToAvro(event);

        SensorEventAvro eventAvro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();

        kafkaClient.sendSensorEvent(eventAvro);
        log.debug("Отправлено событие датчика {} в топик {}", event.getType(), sensorsTopic);
    }

    protected abstract T mapToAvro(SensorEvent event);
}