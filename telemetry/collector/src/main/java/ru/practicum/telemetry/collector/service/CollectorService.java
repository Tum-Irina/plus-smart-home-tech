package ru.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.practicum.telemetry.collector.kafka.KafkaClient;
import org.springframework.stereotype.Service;
import ru.practicum.telemetry.collector.model.hub.HubEvent;
import ru.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.practicum.telemetry.collector.service.mapper.HubEventMapper;
import ru.practicum.telemetry.collector.service.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class CollectorService {

    private final KafkaClient kafkaClient;
    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;

    public void processSensorEvent(SensorEvent event) {
        log.info("Получено событие датчика: type={}, id={}, hubId={}",
                event.getType(), event.getId(), event.getHubId());

        try {
            SensorEventAvro avroEvent = sensorEventMapper.toAvro(event);
            kafkaClient.sendSensorEvent(avroEvent);
            log.debug("Событие датчика успешно обработано и отправлено в Kafka");
        } catch (Exception e) {
            log.error("Ошибка обработки события датчика: {}", e.getMessage(), e);
            throw e;
        }
    }

    public void processHubEvent(HubEvent event) {
        log.info("Получено событие хаба: type={}, hubId={}",
                event.getType(), event.getHubId());

        try {
            HubEventAvro avroEvent = hubEventMapper.toAvro(event);
            kafkaClient.sendHubEvent(avroEvent);
            log.debug("Событие хаба успешно обработано и отправлено в Kafka");
        } catch (Exception e) {
            log.error("Ошибка обработки события хаба: {}", e.getMessage(), e);
            throw e;
        }
    }
}