package ru.practicum.telemetry.collector.service.handle.grpc.hub;

import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.practicum.telemetry.collector.service.handle.grpc.BaseHubGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Component
public class ScenarioAddedHubGrpcEventHandler extends BaseHubGrpcEventHandler<ScenarioAddedEventAvro> {

    public ScenarioAddedHubGrpcEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvroPayload(HubEventProto event) {
        ScenarioAddedEventProto scenarioAdded = event.getScenarioAdded();

        List<ScenarioConditionAvro> conditions = scenarioAdded.getConditionList().stream()
                .map(this::mapCondition)
                .toList();

        List<DeviceActionAvro> actions = scenarioAdded.getActionList().stream()
                .map(this::mapAction)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAdded.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    private ScenarioConditionAvro mapCondition(ScenarioConditionProto condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(mapConditionType(condition.getType()))
                .setOperation(mapConditionOperation(condition.getOperation()));

        switch (condition.getValueCase()) {
            case BOOL_VALUE:
                builder.setValue(condition.getBoolValue());
                break;
            case INT_VALUE:
                builder.setValue(condition.getIntValue());
                break;
            default:
                builder.setValue(null);
        }

        return builder.build();
    }

    private DeviceActionAvro mapAction(DeviceActionProto action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(mapActionType(action.getType()));

        if (action.hasValue()) {
            builder.setValue(action.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }

    private ConditionTypeAvro mapConditionType(ConditionTypeProto type) {
        return ConditionTypeAvro.valueOf(type.name());
    }

    private ConditionOperationAvro mapConditionOperation(ConditionOperationProto operation) {
        return ConditionOperationAvro.valueOf(operation.name());
    }

    private ActionTypeAvro mapActionType(ActionTypeProto type) {
        return ActionTypeAvro.valueOf(type.name());
    }
}