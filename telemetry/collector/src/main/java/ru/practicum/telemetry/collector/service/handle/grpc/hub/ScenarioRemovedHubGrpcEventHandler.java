package ru.practicum.telemetry.collector.service.handle.grpc.hub;

import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.practicum.telemetry.collector.service.handle.grpc.BaseHubGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

@Component
public class ScenarioRemovedHubGrpcEventHandler extends BaseHubGrpcEventHandler<ScenarioRemovedEventAvro> {

    public ScenarioRemovedHubGrpcEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    protected ScenarioRemovedEventAvro mapToAvroPayload(HubEventProto event) {
        ScenarioRemovedEventProto scenarioRemoved = event.getScenarioRemoved();

        return ScenarioRemovedEventAvro.newBuilder()
                .setName(scenarioRemoved.getName())
                .build();
    }
}