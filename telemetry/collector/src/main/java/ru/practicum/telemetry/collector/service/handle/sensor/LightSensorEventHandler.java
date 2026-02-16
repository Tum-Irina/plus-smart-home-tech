package ru.practicum.telemetry.collector.service.handle.sensor;

import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.model.sensor.LightSensorEvent;
import ru.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.practicum.telemetry.collector.service.handle.BaseSensorEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;

@Component("LIGHT_SENSOR_EVENT")
public class LightSensorEventHandler extends BaseSensorEventHandler<LightSensorAvro> {

    public LightSensorEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }

    @Override
    protected LightSensorAvro mapToAvro(SensorEvent event) {
        LightSensorEvent lightEvent = (LightSensorEvent) event;
        return LightSensorAvro.newBuilder()
                .setLinkQuality(lightEvent.getLinkQuality())
                .setLuminosity(lightEvent.getLuminosity())
                .build();
    }
}