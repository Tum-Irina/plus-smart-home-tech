package ru.practicum.telemetry.collector.service.handle.grpc;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

public interface HubGrpcEventHandler {
    HubEventProto.PayloadCase getMessageType();

    void handle(HubEventProto event);
}