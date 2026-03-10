package ru.practicum.telemetry.collector.service.handle.grpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseHubGrpcEventHandler<T extends SpecificRecordBase> implements HubGrpcEventHandler {

    private final KafkaClient kafkaClient;

    @Override
    public void handle(HubEventProto event) {
        if (event.getPayloadCase() != getMessageType()) {
            throw new IllegalArgumentException(
                    String.format("Неверный тип события. Ожидалось: %s, получено: %s",
                            getMessageType(), event.getPayloadCase())
            );
        }

        T payload = mapToAvroPayload(event);

        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(convertTimestampToInstant(event.getTimestamp()))
                .setPayload(payload)
                .build();

        kafkaClient.sendHubEvent(eventAvro);
        log.debug("Отправлено событие хаба {} в Kafka", event.getPayloadCase());
    }

    protected abstract T mapToAvroPayload(HubEventProto event);

    private java.time.Instant convertTimestampToInstant(com.google.protobuf.Timestamp timestamp) {
        return java.time.Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}