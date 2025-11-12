package com.gabrielrq.database_converter.controller;

import com.gabrielrq.database_converter.domain.MigrationStatus;
import com.gabrielrq.database_converter.dto.SqlDTO;
import com.gabrielrq.database_converter.dto.SqlPageDTO;
import com.gabrielrq.database_converter.service.SqlService;
import com.gabrielrq.database_converter.service.etl.EtlService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/sql")
public class SqlController {

    private final SqlService sqlService;
    private final EtlService etlService;

    public SqlController(SqlService sqlService, EtlService etlService) {
        this.sqlService = sqlService;
        this.etlService = etlService;
    }

    @GetMapping("/{id}")
    public ResponseEntity<SqlPageDTO> listSql(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "5") int size
    ) {
        MigrationStatus status = etlService.getCurrentStatus(id);
        return ResponseEntity.ok(sqlService.listDDL(status.getName(), page, size));
    }

    @PutMapping("/update/{id}")
    public ResponseEntity<Void> updateSqlFile(@PathVariable UUID id, @RequestBody SqlDTO sqlDTO) {
        MigrationStatus status = etlService.getCurrentStatus(id);
        sqlService.updateDDL(status.getName(), sqlDTO.filename(), sqlDTO.content());
        return ResponseEntity.noContent().build();
    }
}
