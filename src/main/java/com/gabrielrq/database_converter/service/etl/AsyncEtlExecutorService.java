package com.gabrielrq.database_converter.service.etl;

import com.gabrielrq.database_converter.domain.DatabaseDefinition;
import com.gabrielrq.database_converter.domain.MigrationStatus;
import com.gabrielrq.database_converter.dto.ConsistencyValidationDataDTO;
import com.gabrielrq.database_converter.dto.EtlRequestDTO;
import com.gabrielrq.database_converter.dto.TransformationResult;
import com.gabrielrq.database_converter.enums.EtlStep;
import com.gabrielrq.database_converter.repository.EtlStatusRepository;
import com.gabrielrq.database_converter.service.ConsistencyValidationService;
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

    public AsyncEtlExecutorService(
            DataExtractionService extractionService,
            DataTransformationService transformationService,
            DataLoadingService loadingService,
            ConsistencyValidationService consistencyValidationService,
            EtlStatusRepository statusRepository
    ) {
        this.extractionService = extractionService;
        this.transformationService = transformationService;
        this.loadingService = loadingService;
        this.consistencyValidationService = consistencyValidationService;
        this.statusRepository = statusRepository;
    }

    @Async
    public void startExtraction(EtlRequestDTO req, MigrationStatus status) {
        status.setStep(EtlStep.EXTRACTION_IN_PROGRESS);
        statusRepository.save(status);

        try {
            DatabaseDefinition metadata = extractionService.extract(req.originConfig());
            status.setMetadata(metadata);
            status.setStep(EtlStep.EXTRACTION_FINISHED);
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        }
    }

    @Async
    public void startTransformation(EtlRequestDTO req, MigrationStatus status) {
        status.setStep(EtlStep.TRANSFORMATION_IN_PROGRESS);
        statusRepository.save(status);

        try {
            TransformationResult result = transformationService.transform(status.getMetadata(), req.target());
            status.setMetadata(result.metadata());
            status.setExecutionOrder(result.executionList());
            status.setStep(EtlStep.TRANSFORMATION_FINISHED);
            statusRepository.save(status);
            status.setStep(EtlStep.WAITING_FOR_LOAD_CONFIRMATION);
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        }
    }

    @Async
    public void startLoading(EtlRequestDTO req, MigrationStatus status) {
        status.setStep(EtlStep.LOAD_IN_PROGRESS);
        statusRepository.save(status);

        try {
            loadingService.load(req.targetConfig(), new TransformationResult(status.getMetadata(), status.getExecutionOrder()));
            status.setStep(EtlStep.LOAD_FINISHED);
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        }
    }

    @Async
    public void startConsistencyValidation(EtlRequestDTO req, MigrationStatus status) {
        status.setStep(EtlStep.VALIDATION_IN_PROGRESS);
        statusRepository.save(status);

        try {
            ConsistencyValidationDataDTO validationData = consistencyValidationService.validate(req.originConfig(), req.targetConfig());
            status.setMessage(String.join(System.lineSeparator(), validationData.messages()));
            status.setStep(EtlStep.FINISHED);
            status.setFinishedAt(LocalDateTime.now());
            statusRepository.save(status);
        } catch (Exception e) {
            status.setStep(EtlStep.ERROR);
            status.setMessage(e.getMessage());
            statusRepository.save(status);
        }
    }
}
