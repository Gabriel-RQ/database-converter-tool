package com.gabrielrq.database_converter.dto;

import java.util.UUID;

public record EtlRequestDTO(
        UUID id,
        DbConnectionConfigDTO config,
        String target
) {
}
