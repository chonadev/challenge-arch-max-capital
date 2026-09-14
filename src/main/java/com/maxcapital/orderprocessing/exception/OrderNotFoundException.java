package com.maxcapital.orderprocessing.exception;

/**
 * Se lanza cuando se consulta una orden que no existe. Es un error de
 * la capa de consulta: la orden puede no haberse creado aun (el primer
 * ER nunca llego) o el id no corresponde a ninguna orden conocida.
 */
public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(Long numericOrderId) {
        super("Orden no encontrada: " + numericOrderId);
    }
}