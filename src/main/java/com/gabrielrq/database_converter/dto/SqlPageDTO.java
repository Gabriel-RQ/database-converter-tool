package com.gabrielrq.database_converter.dto;

import java.util.List;

public record SqlPageDTO(
        int page,
        int size,
        int total,
        List<SqlDTO> files
) {
}
