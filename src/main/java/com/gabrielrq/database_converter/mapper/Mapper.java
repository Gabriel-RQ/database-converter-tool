package com.gabrielrq.database_converter.mapper;

import com.gabrielrq.database_converter.domain.MigrationStatus;
import com.gabrielrq.database_converter.dto.MigrationStatusDTO;

public class Mapper {

    public static MigrationStatusDTO toMigrationStatusDTO(MigrationStatus status) {
        return new MigrationStatusDTO(
                status.getId(),
                status.getName(),
                status.getStep(),
                status.getMessage(),
                status.getStartedAt(),
                status.getFinishedAt(),
                status.getLastUpdated()
        );
    }
}
