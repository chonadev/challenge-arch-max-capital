package com.maxcapital.orderprocessing.service;

import com.maxcapital.orderprocessing.dto.OrderResponse;
import com.maxcapital.orderprocessing.exception.OrderNotFoundException;
import com.maxcapital.orderprocessing.model.Order;
import com.maxcapital.orderprocessing.model.ExecutionLedger;
import com.maxcapital.orderprocessing.repository.ExecutionLedgerRepository;
import com.maxcapital.orderprocessing.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrderQueryService {

    private final OrderRepository orderRepository;
    private final ExecutionLedgerRepository ledgerRepository;

    /**
     * Consulta el estado actual de una orden y su ledger paginado.
     * Es una lectura pura: no usa el lock pesimista de
     * findByIdForUpdate ni participa de la transaccion de aplicacion
     * de ER de OrderProcessingService. El ledger se pagina por id
     * ascendente (orden de insercion) SIEMPRE: el sort del cliente es
     * descartado a proposito, para que el contrato de orden no dependa
     * del llamador.
     */
    @Transactional(readOnly = true)
    public OrderResponse getOrder(Long numericOrderId, Pageable pageable) {
        Order order = orderRepository.findById(numericOrderId)
            .orElseThrow(() -> new OrderNotFoundException(numericOrderId));

        Page<ExecutionLedger> ledgerPage = ledgerRepository.findByNumericOrderId(
            numericOrderId,
            PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(),
                Sort.by(Sort.Direction.ASC, "id"))
        );

        return OrderResponse.from(order, ledgerPage);
    }
}