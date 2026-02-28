package ru.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.telemetry.analyzer.entity.*;
import ru.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ScenarioAnalyzer {

    private final ScenarioRepository scenarioRepository;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public void analyze(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> sensors = snapshot.getSensorsState();

        List<Scenario> scenariosWithConditions = scenarioRepository.findByHubIdWithConditions(hubId);

        if (scenariosWithConditions.isEmpty()) {
            return;
        }

        List<Long> scenarioIds = scenariosWithConditions.stream()
                .map(Scenario::getId)
                .collect(Collectors.toList());

        Map<Long, List<ScenarioAction>> actionsByScenario =
                scenarioRepository.findActionsByScenarioIds(scenarioIds).stream()
                        .collect(Collectors.groupingBy(sa -> sa.getScenario().getId()));

        for (Scenario scenario : scenariosWithConditions) {
            List<ScenarioAction> actions = actionsByScenario.getOrDefault(scenario.getId(), List.of());

            boolean conditionsMet = checkConditions(scenario, sensors);

            if (conditionsMet) {
                log.info("Сценарий '{}' для хаба {} активирован", scenario.getName(), hubId);
                executeActions(actions, scenario.getName(), hubId, snapshot.getTimestamp());
            }
        }
    }

    private void executeActions(List<ScenarioAction> actions, String scenarioName,
                                String hubId, Instant eventTime) {
        for (ScenarioAction scenarioAction : actions) {
            try {
                sendAction(scenarioAction, scenarioName, hubId, eventTime);
            } catch (StatusRuntimeException e) {
                log.error("Ошибка при отправке действия для сценария '{}': {}",
                        scenarioName, e.getMessage());
            }
        }
    }

    private boolean checkConditions(Scenario scenario, Map<String, SensorStateAvro> sensors) {

        List<ScenarioCondition> scenarioConditions = scenario.getScenarioConditions();

        if (scenarioConditions.isEmpty()) {
            return true;
        }

        return scenarioConditions.stream()
                .allMatch(sc -> checkSingleCondition(sc, sensors));
    }

    private boolean checkSingleCondition(ScenarioCondition scenarioCondition,
                                         Map<String, SensorStateAvro> sensors) {
        Condition condition = scenarioCondition.getCondition();
        String sensorId = scenarioCondition.getSensor().getId();

        SensorStateAvro sensorState = sensors.get(sensorId);
        if (sensorState == null) {
            log.debug("Датчик {} не найден в снапшоте", sensorId);
            return false;
        }

        int actualValue = extractValue(sensorState.getData(), condition.getType());
        int expectedValue = condition.getValue();

        return compareValues(actualValue, expectedValue, condition.getOperation());
    }

    private int extractValue(Object data, Condition.ConditionType type) {
        switch (type) {
            case MOTION:
                return ((MotionSensorAvro) data).getMotion() ? 1 : 0;
            case SWITCH:
                return ((SwitchSensorAvro) data).getState() ? 1 : 0;
            case LUMINOSITY:
                return ((LightSensorAvro) data).getLuminosity();
            case TEMPERATURE:
                if (data instanceof ClimateSensorAvro) {
                    return ((ClimateSensorAvro) data).getTemperatureC();
                } else if (data instanceof TemperatureSensorAvro) {
                    return ((TemperatureSensorAvro) data).getTemperatureC();
                }
                break;
            case CO2LEVEL:
                return ((ClimateSensorAvro) data).getCo2Level();
            case HUMIDITY:
                return ((ClimateSensorAvro) data).getHumidity();
        }
        throw new IllegalArgumentException("Неизвестный тип условия: " + type +
                " для данных: " + data.getClass().getSimpleName());
    }

    private boolean compareValues(int actual, int expected,
                                  Condition.ConditionOperation operation) {
        switch (operation) {
            case EQUALS:
                return actual == expected;
            case GREATER_THAN:
                return actual > expected;
            case LOWER_THAN:
                return actual < expected;
            default:
                throw new IllegalArgumentException("Неизвестная операция: " + operation);
        }
    }

    private void executeActions(Scenario scenario, String hubId, Instant eventTime) {
        List<ScenarioAction> scenarioActions = scenario.getScenarioActions();

        for (ScenarioAction scenarioAction : scenarioActions) {
            try {
                sendAction(scenarioAction, scenario.getName(), hubId, eventTime);
            } catch (StatusRuntimeException e) {
                log.error("Ошибка при отправке действия для сценария '{}': {}",
                        scenario.getName(), e.getMessage());
            }
        }
    }

    private void sendAction(ScenarioAction scenarioAction, String scenarioName,
                            String hubId, Instant eventTime) {
        Action action = scenarioAction.getAction();
        String sensorId = scenarioAction.getSensor().getId();

        DeviceActionProto deviceAction = DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(mapActionType(action.getType()))
                .setValue(action.getValue() != null ? action.getValue() : 0)
                .build();

        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(deviceAction)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(eventTime.getEpochSecond())
                        .setNanos(eventTime.getNano())
                        .build())
                .build();

        hubRouterClient.handleDeviceAction(request);
        log.debug("Отправлено действие {} для датчика {} в сценарии '{}'",
                action.getType(), sensorId, scenarioName);
    }

    private ActionTypeProto mapActionType(Action.ActionType type) {
        switch (type) {
            case ACTIVATE:
                return ActionTypeProto.ACTIVATE;
            case DEACTIVATE:
                return ActionTypeProto.DEACTIVATE;
            case INVERSE:
                return ActionTypeProto.INVERSE;
            case SET_VALUE:
                return ActionTypeProto.SET_VALUE;
            default:
                throw new IllegalArgumentException("Неизвестный тип действия: " + type);
        }
    }
}