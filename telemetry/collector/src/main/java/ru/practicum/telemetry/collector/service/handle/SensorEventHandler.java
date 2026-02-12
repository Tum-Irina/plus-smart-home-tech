package ru.practicum.telemetry.collector.service.handle;

import ru.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.practicum.telemetry.collector.model.sensor.SensorEventType;

public interface SensorEventHandler {
    SensorEventType getMessageType();

    void handle(SensorEvent event);
}