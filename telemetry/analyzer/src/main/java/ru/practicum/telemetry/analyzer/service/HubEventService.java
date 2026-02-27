package ru.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.telemetry.analyzer.entity.*;
import ru.practicum.telemetry.analyzer.repository.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.Optional;

@Slf4j
@Service
@Transactional
@RequiredArgsConstructor
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    public void process(HubEventAvro event) {
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro) {
            handleDeviceAdded(event.getHubId(), (DeviceAddedEventAvro) payload);
        } else if (payload instanceof DeviceRemovedEventAvro) {
            handleDeviceRemoved((DeviceRemovedEventAvro) payload);
        } else if (payload instanceof ScenarioAddedEventAvro) {
            handleScenarioAdded(event.getHubId(), (ScenarioAddedEventAvro) payload);
        } else if (payload instanceof ScenarioRemovedEventAvro) {
            handleScenarioRemoved(event.getHubId(), (ScenarioRemovedEventAvro) payload);
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        Sensor sensor = new Sensor();
        sensor.setId(event.getId());
        sensor.setHubId(hubId);
        sensorRepository.save(sensor);
        log.info("Добавлен датчик {} для хаба {}", event.getId(), hubId);
    }

    private void handleDeviceRemoved(DeviceRemovedEventAvro event) {
        sensorRepository.deleteById(event.getId());
        log.info("Удален датчик {}", event.getId());
    }

    protected void handleScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        Optional<Scenario> existing = scenarioRepository.findByHubIdAndName(hubId, event.getName());

        if (existing.isPresent()) {
            updateScenario(existing.get(), event);
        } else {
            createScenario(hubId, event);
        }
    }

    private void createScenario(String hubId, ScenarioAddedEventAvro event) {

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName());
        scenario = scenarioRepository.save(scenario);

        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {

            Condition condition = new Condition();
            condition.setType(Condition.ConditionType.valueOf(conditionAvro.getType().name()));
            condition.setOperation(Condition.ConditionOperation.valueOf(conditionAvro.getOperation().name()));

            Object value = conditionAvro.getValue();
            if (value instanceof Integer) {
                condition.setValue((Integer) value);
            } else if (value instanceof Boolean) {
                condition.setValue((Boolean) value ? 1 : 0);
            }
            condition = conditionRepository.save(condition);

            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId())
                    .orElseThrow(() -> new RuntimeException(
                            "Датчик не найден: " + conditionAvro.getSensorId()));

            ScenarioConditionId scId = new ScenarioConditionId();
            scId.setScenarioId(scenario.getId());
            scId.setSensorId(sensor.getId());
            scId.setConditionId(condition.getId());

            ScenarioCondition scenarioCondition = new ScenarioCondition();
            scenarioCondition.setId(scId);
            scenarioCondition.setScenario(scenario);
            scenarioCondition.setSensor(sensor);
            scenarioCondition.setCondition(condition);

            scenario.getScenarioConditions().add(scenarioCondition);
        }

        for (DeviceActionAvro actionAvro : event.getActions()) {
            Action action = new Action();
            action.setType(Action.ActionType.valueOf(actionAvro.getType().name()));
            if (actionAvro.getValue() != null) {
                action.setValue(actionAvro.getValue());
            }
            action = actionRepository.save(action);

            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId())
                    .orElseThrow(() -> new RuntimeException(
                            "Датчик не найден: " + actionAvro.getSensorId()));

            ScenarioActionId saId = new ScenarioActionId();
            saId.setScenarioId(scenario.getId());
            saId.setSensorId(sensor.getId());
            saId.setActionId(action.getId());

            ScenarioAction scenarioAction = new ScenarioAction();
            scenarioAction.setId(saId);
            scenarioAction.setScenario(scenario);
            scenarioAction.setSensor(sensor);
            scenarioAction.setAction(action);

            scenario.getScenarioActions().add(scenarioAction);
        }

        scenarioRepository.save(scenario);
        log.info("Добавлен сценарий {} для хаба {}", event.getName(), hubId);
    }

    private void updateScenario(Scenario scenario, ScenarioAddedEventAvro event) {

        scenario.getScenarioConditions().clear();
        scenario.getScenarioActions().clear();

        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            Condition condition = new Condition();
            condition.setType(Condition.ConditionType.valueOf(conditionAvro.getType().name()));
            condition.setOperation(Condition.ConditionOperation.valueOf(conditionAvro.getOperation().name()));

            Object value = conditionAvro.getValue();
            if (value instanceof Integer) {
                condition.setValue((Integer) value);
            } else if (value instanceof Boolean) {
                condition.setValue((Boolean) value ? 1 : 0);
            }
            condition = conditionRepository.save(condition);

            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId())
                    .orElseThrow(() -> new RuntimeException(
                            "Датчик не найден: " + conditionAvro.getSensorId()));

            ScenarioConditionId scId = new ScenarioConditionId();
            scId.setScenarioId(scenario.getId());
            scId.setSensorId(sensor.getId());
            scId.setConditionId(condition.getId());

            ScenarioCondition scenarioCondition = new ScenarioCondition();
            scenarioCondition.setId(scId);
            scenarioCondition.setScenario(scenario);
            scenarioCondition.setSensor(sensor);
            scenarioCondition.setCondition(condition);

            scenario.getScenarioConditions().add(scenarioCondition);
        }

        for (DeviceActionAvro actionAvro : event.getActions()) {
            Action action = new Action();
            action.setType(Action.ActionType.valueOf(actionAvro.getType().name()));
            if (actionAvro.getValue() != null) {
                action.setValue(actionAvro.getValue());
            }
            action = actionRepository.save(action);

            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId())
                    .orElseThrow(() -> new RuntimeException(
                            "Датчик не найден: " + actionAvro.getSensorId()));

            ScenarioActionId saId = new ScenarioActionId();
            saId.setScenarioId(scenario.getId());
            saId.setSensorId(sensor.getId());
            saId.setActionId(action.getId());

            ScenarioAction scenarioAction = new ScenarioAction();
            scenarioAction.setId(saId);
            scenarioAction.setScenario(scenario);
            scenarioAction.setSensor(sensor);
            scenarioAction.setAction(action);

            scenario.getScenarioActions().add(scenarioAction);
        }

        scenarioRepository.save(scenario);
        log.info("Обновлен сценарий {} для хаба {}", event.getName(), scenario.getHubId());
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    log.info("Удален сценарий {} для хаба {}", event.getName(), hubId);
                });
    }
}