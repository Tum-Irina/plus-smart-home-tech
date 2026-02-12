package ru.practicum.telemetry.collector.service.handle.hub;

import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.model.hub.DeviceRemovedEvent;
import ru.practicum.telemetry.collector.model.hub.HubEvent;
import ru.practicum.telemetry.collector.model.hub.HubEventType;
import ru.practicum.telemetry.collector.service.handle.BaseHubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;

@Component("DEVICE_REMOVED")
public class DeviceRemovedHubEventHandler extends BaseHubEventHandler<DeviceRemovedEventAvro> {

    public DeviceRemovedHubEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(HubEvent event) {
        DeviceRemovedEvent removedEvent = (DeviceRemovedEvent) event;
        return DeviceRemovedEventAvro.newBuilder()
                .setId(removedEvent.getId())
                .build();
    }
}