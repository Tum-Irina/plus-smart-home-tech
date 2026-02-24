package ru.practicum.telemetry.collector.service.handle.grpc.sensor;

import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.practicum.telemetry.collector.service.handle.grpc.BaseSensorGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;

@Component
public class LightSensorGrpcEventHandler extends BaseSensorGrpcEventHandler<LightSensorAvro> {

    public LightSensorGrpcEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    protected LightSensorAvro mapToAvroPayload(SensorEventProto event) {
        LightSensorProto lightSensor = event.getLightSensor();
        return LightSensorAvro.newBuilder()
                .setLinkQuality(lightSensor.getLinkQuality())
                .setLuminosity(lightSensor.getLuminosity())
                .build();
    }
}