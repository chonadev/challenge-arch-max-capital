package com.maxcapital.orderprocessing.exception;

/**
 * Error permanente: el mensaje esta mal formado o viola una invariante
 * de negocio que reintentar no va a resolver (ej: transicion de estado
 * invalida). Va directo a DLQ, no se reintenta.
 */
public class PermanentProcessingException extends RuntimeException {

    public PermanentProcessingException(String message) {
        super(message);
    }

    public PermanentProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
