package com.maxcapital.orderprocessing.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

@Slf4j
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ApiError> handleOrderNotFound(OrderNotFoundException e) {
        return build(HttpStatusCode.valueOf(404), "NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleBadRequest(IllegalArgumentException e) {
        return build(HttpStatusCode.valueOf(400), "BAD_REQUEST", e.getMessage());
    }

    /**
     * Preserva el comportamiento existente de los controllers que aun
     * usan ResponseStatusException (p. ej. TestSeedController), en vez
     * de dejarlos caer en el handler generico de 500.
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiError> handleResponseStatus(ResponseStatusException e) {
        return build(e.getStatusCode(), "REQUEST_FAILED", e.getReason());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleGeneric(Exception e) {
        log.error("Error no manejado en el controller", e);
        return build(HttpStatusCode.valueOf(500), "INTERNAL_ERROR", "Error interno del servidor");
    }

    private ResponseEntity<ApiError> build(HttpStatusCode status, String error, String message) {
        return ResponseEntity.status(status).body(new ApiError(error, message));
    }
}