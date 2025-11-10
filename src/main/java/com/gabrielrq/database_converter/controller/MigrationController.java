package com.gabrielrq.database_converter.controller;

import com.gabrielrq.database_converter.domain.MigrationStatus;
import com.gabrielrq.database_converter.dto.EtlRequestDTO;
import com.gabrielrq.database_converter.dto.MigrationStatusDTO;
import com.gabrielrq.database_converter.dto.StartMigrationRequestDTO;
import com.gabrielrq.database_converter.mapper.Mapper;
import com.gabrielrq.database_converter.service.etl.EtlService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
@RequestMapping("/api/v1/migration")
public class MigrationController {

    private final EtlService etlService;

    public MigrationController(EtlService etlService) {
        this.etlService = etlService;
    }

    @PostMapping("/new")
    public ResponseEntity<MigrationStatusDTO> newMigration(@RequestBody StartMigrationRequestDTO startMigrationRequestDTO) {
        MigrationStatus status = etlService.createNew(startMigrationRequestDTO.name());
        return ResponseEntity.ok(Mapper.toMigrationStatusDTO(status));
    }

    @PostMapping("/extract")
    public ResponseEntity<Void> startExtraction(@RequestBody EtlRequestDTO etlRequestDTO) {
        etlService.startExtraction(etlRequestDTO);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/transform")
    public ResponseEntity<Void> startTransformation(@RequestBody EtlRequestDTO etlRequestDTO) {
        etlService.startTransformation(etlRequestDTO);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/load")
    public ResponseEntity<Void> startLoad(@RequestBody EtlRequestDTO etlRequestDTO) {
        etlService.startLoad(etlRequestDTO);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/status/{id}")
    public ResponseEntity<MigrationStatusDTO> getStatus(@PathVariable UUID id) {
        MigrationStatus status = etlService.getCurrentStatus(id);
        return ResponseEntity.ok(Mapper.toMigrationStatusDTO(status));
    }
}
