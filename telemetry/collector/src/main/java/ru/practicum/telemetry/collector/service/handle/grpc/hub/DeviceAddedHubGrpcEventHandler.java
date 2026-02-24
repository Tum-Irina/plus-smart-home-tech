package ru.practicum.telemetry.collector.service.handle.grpc.hub;

import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.practicum.telemetry.collector.service.handle.grpc.BaseHubGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;

@Component
public class DeviceAddedHubGrpcEventHandler extends BaseHubGrpcEventHandler<DeviceAddedEventAvro> {

    public DeviceAddedHubGrpcEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    protected DeviceAddedEventAvro mapToAvroPayload(HubEventProto event) {
        DeviceAddedEventProto deviceAdded = event.getDeviceAdded();
        return DeviceAddedEventAvro.newBuilder()
                .setId(deviceAdded.getId())
                .setType(mapDeviceType(deviceAdded.getType()))
                .build();
    }

    private DeviceTypeAvro mapDeviceType(DeviceTypeProto type) {
        return DeviceTypeAvro.valueOf(type.name());
    }
}