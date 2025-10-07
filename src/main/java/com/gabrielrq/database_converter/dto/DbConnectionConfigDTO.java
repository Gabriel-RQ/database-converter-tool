package com.gabrielrq.database_converter.dto;

public record DbConnectionConfigDTO(
        String name,
        String jdbcUrl,
        String username,
        String password,
        String driverClassName
) {
}
