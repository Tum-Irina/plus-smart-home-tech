package ru.practicum.interaction.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SortObject {
    private String direction;
    private String property;
    private boolean ascending;
    private boolean ignoreCase;
    private String nullHandling;
}