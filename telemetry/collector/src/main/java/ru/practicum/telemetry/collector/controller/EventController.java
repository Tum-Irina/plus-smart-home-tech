package ru.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.telemetry.collector.model.hub.HubEvent;
import ru.practicum.telemetry.collector.model.hub.HubEventType;
import ru.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.practicum.telemetry.collector.service.handle.HubEventHandler;
import ru.practicum.telemetry.collector.service.handle.SensorEventHandler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Validated
@RestController
@RequestMapping(path = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
public class EventController {

    private final Map<SensorEventType, SensorEventHandler> sensorEventHandlers;
    private final Map<HubEventType, HubEventHandler> hubEventHandlers;

    public EventController(List<SensorEventHandler> sensorEventHandlers,
                           Set<HubEventHandler> hubEventHandlers) {
        this.sensorEventHandlers = sensorEventHandlers.stream()
                .collect(Collectors.toMap(
                        SensorEventHandler::getMessageType,
                        Function.identity()
                ));
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(
                        HubEventHandler::getMessageType,
                        Function.identity()
                ));

        log.info("Инициализировано обработчиков: датчики - {}, хабы - {}",
                this.sensorEventHandlers.size(), this.hubEventHandlers.size());
    }

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody SensorEvent request) {
        log.debug("Получен запрос POST /events/sensors, тип: {}", request.getType());

        SensorEventHandler handler = sensorEventHandlers.get(request.getType());
        if (handler == null) {
            throw new IllegalArgumentException(
                    String.format("Не найден обработчик для события датчика: %s", request.getType())
            );
        }

        handler.handle(request);
        log.info("Событие датчика {} обработано", request.getType());
    }

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent request) {
        log.debug("Получен запрос POST /events/hubs, тип: {}", request.getType());

        HubEventHandler handler = hubEventHandlers.get(request.getType());
        if (handler == null) {
            throw new IllegalArgumentException(
                    String.format("Не найден обработчик для события хаба: %s", request.getType())
            );
        }

        handler.handle(request);
        log.info("Событие хаба {} обработано", request.getType());
    }
}