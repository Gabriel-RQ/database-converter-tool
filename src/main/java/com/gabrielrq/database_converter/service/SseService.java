package com.gabrielrq.database_converter.service;

import com.gabrielrq.database_converter.domain.MigrationStatus;
import com.gabrielrq.database_converter.exception.NonExistingSseEmitterException;
import com.gabrielrq.database_converter.exception.SseException;
import com.gabrielrq.database_converter.mapper.MigrationStatusMapper;
import com.gabrielrq.database_converter.repository.SseEmitterRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.UUID;

@Service
public class SseService {

    private final SseEmitterRepository sseEmitterRepository;

    public SseService(SseEmitterRepository sseEmitterRepository) {
        this.sseEmitterRepository = sseEmitterRepository;
    }

    public SseEmitter registerEmitter(UUID id) {
        SseEmitter emitter = new SseEmitter(0L);

        emitter.onCompletion(() -> sseEmitterRepository.delete(id));
        emitter.onTimeout(() -> sseEmitterRepository.delete(id));
        emitter.onError((e) -> sseEmitterRepository.delete(id));

        sendRegistrationConfirmation(emitter);

        sseEmitterRepository.save(id, emitter);
        return emitter;
    }

    private void sendRegistrationConfirmation(SseEmitter emitter) {
        try {
            emitter.send("Emissor de SSE registrado");
        } catch (IOException ignored) {
        }
    }

    public void sendMigrationStatusUpdate(MigrationStatus status) {

        try {
            SseEmitter emitter = sseEmitterRepository.find(status.getId());
            emitter.send(SseEmitter.event().name("status").data(MigrationStatusMapper.toMigrationStatusDTO(status), MediaType.APPLICATION_JSON).build());
        } catch (IOException e) {
            throw new SseException("Erro ao enviar evento SSE. Detalhes: " + e.getMessage());
        } catch (NonExistingSseEmitterException ignored) {
        }
    }

    public void sendSseCompletion(UUID id) {
        SseEmitter emitter = sseEmitterRepository.find(id);
        emitter.complete();
    }
}
