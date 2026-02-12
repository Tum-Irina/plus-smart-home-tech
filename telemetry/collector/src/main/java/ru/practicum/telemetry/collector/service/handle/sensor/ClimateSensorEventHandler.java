package ru.practicum.telemetry.collector.service.handle.sensor;

import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.model.sensor.ClimateSensorEvent;
import ru.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.practicum.telemetry.collector.service.handle.BaseSensorEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;

@Component("CLIMATE_SENSOR_EVENT")
public class ClimateSensorEventHandler extends BaseSensorEventHandler<ClimateSensorAvro> {

    public ClimateSensorEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }

    @Override
    protected ClimateSensorAvro mapToAvro(SensorEvent event) {
        ClimateSensorEvent climateEvent = (ClimateSensorEvent) event;
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(climateEvent.getTemperatureC())
                .setHumidity(climateEvent.getHumidity())
                .setCo2Level(climateEvent.getCo2Level())
                .build();
    }
}