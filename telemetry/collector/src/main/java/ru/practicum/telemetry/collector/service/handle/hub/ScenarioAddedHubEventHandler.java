package ru.practicum.telemetry.collector.service.handle.hub;

import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.model.hub.*;
import ru.practicum.telemetry.collector.service.handle.BaseHubEventHandler;
import ru.practicum.telemetry.collector.service.mapper.EnumMapper;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Component("SCENARIO_ADDED")
public class ScenarioAddedHubEventHandler extends BaseHubEventHandler<ScenarioAddedEventAvro> {

    public ScenarioAddedHubEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvro(HubEvent event) {
        ScenarioAddedEvent scenarioEvent = (ScenarioAddedEvent) event;

        List<ScenarioConditionAvro> conditions = scenarioEvent.getConditions().stream()
                .map(this::mapCondition)
                .toList();

        List<DeviceActionAvro> actions = scenarioEvent.getActions().stream()
                .map(this::mapAction)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioEvent.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    private ScenarioConditionAvro mapCondition(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(EnumMapper.map(condition.getType(), ConditionTypeAvro.class))
                .setOperation(EnumMapper.map(condition.getOperation(), ConditionOperationAvro.class));

        if (condition.getValue() != null) {
            builder.setValue(condition.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }

    private DeviceActionAvro mapAction(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(EnumMapper.map(action.getType(), ActionTypeAvro.class));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }
}