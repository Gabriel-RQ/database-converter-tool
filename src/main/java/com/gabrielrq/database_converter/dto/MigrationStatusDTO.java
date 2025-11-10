package com.gabrielrq.database_converter.dto;

import com.gabrielrq.database_converter.enums.EtlStep;

import java.time.LocalDateTime;
import java.util.UUID;

public record MigrationStatusDTO(
        UUID id,
        String name,
        EtlStep step,
        String message,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        LocalDateTime lastUpdatedAt
) {
}
