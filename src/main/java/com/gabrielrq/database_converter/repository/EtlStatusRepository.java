package com.gabrielrq.database_converter.repository;


import com.gabrielrq.database_converter.domain.MigrationStatus;
import com.gabrielrq.database_converter.exception.NonExistentMigrationException;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class EtlStatusRepository {

    Map<UUID, MigrationStatus> repo = new ConcurrentHashMap<>();

    public synchronized void save(MigrationStatus status) {
        status.setLastUpdated(LocalDateTime.now());
        repo.put(status.getId(), status);
    }

    public MigrationStatus find(UUID id) {
        if (!repo.containsKey(id)) {
            throw new NonExistentMigrationException("Migração com ID '" + id + "' não encontrada.");
        }
        return repo.get(id);
    }
}
