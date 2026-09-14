package com.maxcapital.orderprocessing.controller;

import com.maxcapital.orderprocessing.dto.OrderResponse;
import com.maxcapital.orderprocessing.service.OrderQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class OrderController {

    private final OrderQueryService orderQueryService;

    /**
     * Consulta el estado actual de una orden y su ledger paginado,
     * siempre en orden de insercion (el sort se fija en el service).
     * Es una operacion de solo lectura, no participa de la transaccion
     * de aplicacion de ER. Query params opcionales: page y size
     * (default 0 y 10).
     */
    @GetMapping("/orders/{orderId}")
    public OrderResponse getOrder(@PathVariable Long orderId,
                                  @PageableDefault(size = 10) Pageable pageable) {
        return orderQueryService.getOrder(orderId, pageable);
    }
}