package ru.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.telemetry.analyzer.entity.Action;

public interface ActionRepository extends JpaRepository<Action, Long> {
}