package ru.practicum.telemetry.collector.service.handle.grpc;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorGrpcEventHandler {
    SensorEventProto.PayloadCase getMessageType();

    void handle(SensorEventProto event);
}