package ru.practicum.telemetry.collector.service.handle.grpc.hub;

import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.practicum.telemetry.collector.service.handle.grpc.BaseHubGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;

@Component
public class DeviceRemovedHubGrpcEventHandler extends BaseHubGrpcEventHandler<DeviceRemovedEventAvro> {

    public DeviceRemovedHubGrpcEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvroPayload(HubEventProto event) {
        DeviceRemovedEventProto deviceRemoved = event.getDeviceRemoved();

        return DeviceRemovedEventAvro.newBuilder()
                .setId(deviceRemoved.getId())
                .build();
    }
}