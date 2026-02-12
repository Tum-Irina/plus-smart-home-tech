package ru.practicum.telemetry.collector.service.handle;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.beans.factory.annotation.Value;
import ru.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {
    private final KafkaClient kafkaClient;

    @Value("${topics.hubs}")
    private String hubsTopic;

    @Override
    public void handle(HubEvent event) {
        if (!event.getType().equals(getMessageType())) {
            throw new IllegalArgumentException(String.format("Неизвестный тип события: %s. Ожидался: %s",
                    event.getType(), getMessageType()));
        }
        T payload = mapToAvro(event);

        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();

        kafkaClient.sendHubEvent(eventAvro);
        log.debug("Отправлено событие хаба {} в топик {}", event.getType(), hubsTopic);
    }

    protected abstract T mapToAvro(HubEvent event);
}