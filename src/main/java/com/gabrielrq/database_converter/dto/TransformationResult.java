package com.gabrielrq.database_converter.dto;

import com.gabrielrq.database_converter.domain.DatabaseDefinition;
import com.gabrielrq.database_converter.domain.TableDefinition;

import java.util.List;

public record TransformationResult(
        DatabaseDefinition metadata,
        List<TableDefinition> executionList
) {
}
