package ru.practicum.telemetry.collector.service.mapper;

import lombok.experimental.UtilityClass;

@UtilityClass
public class EnumMapper {

    public <E extends Enum<E>, T extends Enum<T>> T map(E source, Class<T> targetType) {
        if (source == null) {
            return null;
        }
        return Enum.valueOf(targetType, source.name());
    }
}