package ru.practicum.telemetry.collector.service.handle.grpc.sensor;

import org.springframework.stereotype.Component;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import ru.practicum.telemetry.collector.service.handle.grpc.BaseSensorGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;

@Component
public class SwitchSensorGrpcEventHandler extends BaseSensorGrpcEventHandler<SwitchSensorAvro> {

    public SwitchSensorGrpcEventHandler(KafkaClient kafkaClient) {
        super(kafkaClient);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR;
    }

    @Override
    protected SwitchSensorAvro mapToAvroPayload(SensorEventProto event) {
        SwitchSensorProto switchSensor = event.getSwitchSensor();
        return SwitchSensorAvro.newBuilder()
                .setState(switchSensor.getState())
                .build();
    }
}