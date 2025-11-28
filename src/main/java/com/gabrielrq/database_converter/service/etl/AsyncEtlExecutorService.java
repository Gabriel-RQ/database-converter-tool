package com.gabrielrq.database_converter.service.etl;

import com.gabrielrq.database_converter.domain.DatabaseDefinition;
import com.gabrielrq.database_converter.domain.MigrationStatus;
import com.gabrielrq.database_converter.dto.ConsistencyValidationDataDTO;
import com.gabrielrq.database_converter.domain.TransformationResult;
import com.gabrielrq.database_converter.enums.EtlStep;
import com.gabrielrq.database_converter.repository.EtlStatusRepository;
import com.gabrielrq.database_converter.service.ConsistencyValidationService;
import com.gabrielrq.database_converter.service.SseService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class AsyncEtlExecutorService {
    private final DataExtractionService extractionService;
    private final DataTransformationService transformationService;
    private final DataLoadingService loadingService;
    private final ConsistencyValidationService consistencyValidationService;
    private final EtlStatusRepository statusRepository;
    private final SseService sseService;

    public AsyncEtlExecutorService(
            DataExtractionService extractionService,
            DataTransformationService transformationService,
            DataLoadingService loadingService,
            ConsistencyValidationService consistencyValidationService,
            EtlStatusRepository statusRepository,
            SseService sseService
    ) {
        this.extractionService = extractionService;
        this.transformationService = transformationService;
        this.loadingService = loadingService;
        this.consistencyValidationService = consistencyValidationService;
        this.statusRepository = statusRepository;
        this.sseService = sseService;
    }

    @Async
    public void startExtraction(MigrationStatus status) {
        sseService.sendMigrationStatusUpdate(status);
        status.setStep(EtlStep.EXTRACTION_IN_PROGRESS);
        statusRepository.save(status);

        try {
            sseService.sendMigrationStatusUpdate(status);
            DatabaseDefinition metadata = extractionService.extract(status.getMetadata().getOriginConfig());
            status.getMetadata().setDatabaseMetadata(metadata);
            status.setStep(EtlStep.EXTRACTION_FINISHED);
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        } finally {
            sseService.sendMigrationStatusUpdate(status);
        }
    }

    @Async
    public void startTransformation(MigrationStatus status) {
        status.setStep(EtlStep.TRANSFORMATION_IN_PROGRESS);
        statusRepository.save(status);

        try {
            sseService.sendMigrationStatusUpdate(status);
            TransformationResult result = transformationService.transform(status.getMetadata().getDatabaseMetadata(), status.getMetadata().getTarget());
            status.getMetadata().setDatabaseMetadata(result.metadata());
            status.getMetadata().setExecutionOrder(result.executionList());
            status.setStep(EtlStep.TRANSFORMATION_FINISHED);
            statusRepository.save(status);
            status.setStep(EtlStep.WAITING_FOR_LOAD_CONFIRMATION);
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        } finally {
            sseService.sendMigrationStatusUpdate(status);
        }
    }

    @Async
    public void startLoading(MigrationStatus status) {
        status.setStep(EtlStep.LOAD_IN_PROGRESS);
        statusRepository.save(status);

        try {
            sseService.sendMigrationStatusUpdate(status);
            loadingService.load(
                    status.getMetadata().getTargetConfig(),
                    new TransformationResult(status.getMetadata().getDatabaseMetadata(), status.getMetadata().getExecutionOrder())
            );
            status.setStep(EtlStep.LOAD_FINISHED);
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        } finally {
            sseService.sendMigrationStatusUpdate(status);
        }
    }

    @Async
    public void startConsistencyValidation(MigrationStatus status) {
        status.setStep(EtlStep.VALIDATION_IN_PROGRESS);
        statusRepository.save(status);

        try {
            sseService.sendMigrationStatusUpdate(status);
            ConsistencyValidationDataDTO validationData = consistencyValidationService.validate(
                    status.getMetadata().getOriginConfig(), status.getMetadata().getTargetConfig()
            );
            status.setMessage(String.join(System.lineSeparator(), validationData.messages()));
            status.setStep(EtlStep.FINISHED);
            status.setFinishedAt(LocalDateTime.now());
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        } finally {
            sseService.sendMigrationStatusUpdate(status);
            sseService.sendSseCompletion(status.getId());
        }
    }
}
