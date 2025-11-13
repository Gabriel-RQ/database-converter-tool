package com.gabrielrq.database_converter.exception;

import com.gabrielrq.database_converter.dto.ErrorResponseDTO;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({RuntimeException.class})
    public ResponseEntity<ErrorResponseDTO> handleRuntimeException(RuntimeException ex, HttpServletRequest request) {
        ErrorResponseDTO error = new ErrorResponseDTO(HttpStatus.UNPROCESSABLE_ENTITY.value(), ex.getMessage(), request.getRequestURI());
        return ResponseEntity.internalServerError().body(error);
    }

    @ExceptionHandler({NonExistentMigrationException.class})
    public ResponseEntity<ErrorResponseDTO> handleNonExistentMigrationException(NonExistentMigrationException ex, HttpServletRequest request) {
        ErrorResponseDTO error = new ErrorResponseDTO(HttpStatus.UNPROCESSABLE_ENTITY.value(), ex.getMessage(), request.getRequestURI());
        return ResponseEntity.badRequest().body(error);
    }

    @ExceptionHandler({InvalidMigrationStateException.class})
    public ResponseEntity<ErrorResponseDTO> handleInvalidMigrationStateException(InvalidMigrationStateException ex, HttpServletRequest request) {
        ErrorResponseDTO error = new ErrorResponseDTO(HttpStatus.UNPROCESSABLE_ENTITY.value(), ex.getMessage(), request.getRequestURI());
        return ResponseEntity.unprocessableEntity().body(error);
    }
}
