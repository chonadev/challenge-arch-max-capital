package com.maxcapital.orderprocessing.exception;

/**
 * Se lanza cuando un ER intenta aplicarse sobre una orden que ya
 * esta en estado terminal (FILLED / CANCELLED). Es un error de
 * dominio, no de infraestructura: vive independiente de Kafka.
 */
public class InvalidStateTransitionException extends RuntimeException {

    public InvalidStateTransitionException(String message) {
        super(message);
    }
}
