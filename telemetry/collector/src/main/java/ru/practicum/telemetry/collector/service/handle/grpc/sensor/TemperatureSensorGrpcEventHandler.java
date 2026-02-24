package ru.practicum.telemetry.collector.service.handle.grpc.sensor;

import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.practicum.telemetry.collector.service.handle.grpc.BaseSensorGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Component
public class TemperatureSensorGrpcEventHandler extends BaseSensorGrpcEventHandler<TemperatureSensorAvro> {

    public TemperatureSensorGrpcEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    protected TemperatureSensorAvro mapToAvroPayload(SensorEventProto event) {
        TemperatureSensorProto tempSensor = event.getTemperatureSensor();
        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(tempSensor.getTemperatureC())
                .setTemperatureF(tempSensor.getTemperatureF())
                .build();
    }
}