package com.maxcapital.orderprocessing.exception;

/**
 * Body estandar de error de la API REST. `error` es un codigo corto
 * (NOT_FOUND, BAD_REQUEST, INTERNAL_ERROR) y `message` el detalle
 * legible para el llamador.
 */
public record ApiError(String error, String message) {
}