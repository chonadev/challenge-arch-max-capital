package com.maxcapital.orderprocessing.controller;

import com.maxcapital.orderprocessing.dto.OrderResponse;
import com.maxcapital.orderprocessing.model.Order;
import com.maxcapital.orderprocessing.repository.ExecutionLedgerRepository;
import com.maxcapital.orderprocessing.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@RestController
@RequiredArgsConstructor
public class OrderController {

    private final OrderRepository orderRepository;
    private final ExecutionLedgerRepository ledgerRepository;

    /**
     * Consulta el estado actual de una orden: status, cantidad de
     * ejecuciones aplicadas, y el detalle de su ledger en orden de
     * insercion. No usa el lock pesimista de findByIdForUpdate: es
     * una lectura, no participa de la transaccion de aplicacion de ER.
     */
    @GetMapping("/orders/{numericOrderId}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable Long numericOrderId) {
        Order order = orderRepository.findById(numericOrderId)
            .orElseThrow(() -> new ResponseStatusException(
                HttpStatus.NOT_FOUND, "Orden no encontrada: " + numericOrderId));

        var ledgerEntries = ledgerRepository.findByNumericOrderIdOrderByIdAsc(numericOrderId);

        return ResponseEntity.ok(OrderResponse.from(order, ledgerEntries));
    }
}
