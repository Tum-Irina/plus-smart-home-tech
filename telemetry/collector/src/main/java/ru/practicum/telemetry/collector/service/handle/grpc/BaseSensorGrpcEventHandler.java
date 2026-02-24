package ru.practicum.telemetry.collector.service.handle.grpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSensorGrpcEventHandler<T extends SpecificRecordBase> implements SensorGrpcEventHandler {

    private final KafkaClient kafkaClient;

    @Override
    public void handle(SensorEventProto event) {
        if (event.getPayloadCase() != getMessageType()) {
            throw new IllegalArgumentException(
                    String.format("Неверный тип события. Ожидалось: %s, получено: %s",
                            getMessageType(), event.getPayloadCase())
            );
        }

        T payload = mapToAvroPayload(event);

        SensorEventAvro eventAvro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(convertTimestampToInstant(event.getTimestamp()))
                .setPayload(payload)
                .build();

        kafkaClient.sendSensorEvent(eventAvro);
        log.debug("Отправлено событие датчика {} в Kafka", event.getPayloadCase());
    }

    protected abstract T mapToAvroPayload(SensorEventProto event);

    private java.time.Instant convertTimestampToInstant(com.google.protobuf.Timestamp timestamp) {
        return java.time.Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}