package ru.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.telemetry.analyzer.entity.Scenario;
import ru.practicum.telemetry.analyzer.entity.ScenarioAction;

import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {

    Optional<Scenario> findByHubIdAndName(String hubId, String name);

    @Query("SELECT DISTINCT s FROM Scenario s " +
            "LEFT JOIN FETCH s.scenarioConditions sc " +
            "LEFT JOIN FETCH sc.condition " +
            "LEFT JOIN FETCH sc.sensor " +
            "WHERE s.hubId = :hubId")
    List<Scenario> findByHubIdWithConditions(@Param("hubId") String hubId);

    @Query("SELECT sa FROM ScenarioAction sa " +
            "LEFT JOIN FETCH sa.action " +
            "LEFT JOIN FETCH sa.sensor " +
            "WHERE sa.id.scenarioId IN :scenarioIds")
    List<ScenarioAction> findActionsByScenarioIds(@Param("scenarioIds") List<Long> scenarioIds);
}