package ru.practicum.telemetry.collector.service.handle.hub;

import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.model.hub.HubEvent;
import ru.practicum.telemetry.collector.model.hub.HubEventType;
import ru.practicum.telemetry.collector.model.hub.ScenarioRemovedEvent;
import ru.practicum.telemetry.collector.service.handle.BaseHubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

@Component("SCENARIO_REMOVED")
public class ScenarioRemovedHubEventHandler extends BaseHubEventHandler<ScenarioRemovedEventAvro> {

    public ScenarioRemovedHubEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_REMOVED;
    }

    @Override
    protected ScenarioRemovedEventAvro mapToAvro(HubEvent event) {
        ScenarioRemovedEvent removedEvent = (ScenarioRemovedEvent) event;
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(removedEvent.getName())
                .build();
    }
}