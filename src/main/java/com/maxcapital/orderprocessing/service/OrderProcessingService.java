package com.maxcapital.orderprocessing.service;

import com.maxcapital.orderprocessing.dto.ExecutionReport;
import com.maxcapital.orderprocessing.model.Order;
import com.maxcapital.orderprocessing.model.OrderStatus;
import com.maxcapital.orderprocessing.model.OutboxEvent;
import com.maxcapital.orderprocessing.repository.ExecutionLedgerRepository;
import com.maxcapital.orderprocessing.repository.OrderRepository;
import com.maxcapital.orderprocessing.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProcessingService {

    private final OrderRepository orderRepository;
    private final ExecutionLedgerRepository ledgerRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * Aplica un ExecutionReport sobre el estado de una orden.
     * Todo ocurre en una sola transaccion:
     *   1. Lock pesimista sobre la orden (segunda linea de defensa
     *      ante ventana de rebalanceo; la primera es el particionado
     *      por numericOrderId en Kafka).
     *   2. Insert idempotente en el ledger via ON CONFLICT DO NOTHING
     *      sobre (secondaryTradeId, operationNumber). Si ya existia
     *      (reentrega), no se toca la orden y se corta aca.
     *   3. Update incremental de la orden: valida transicion de status
     *      contra el status ya persistido, incrementa executionsCount.
     *   4. Si el nuevo status es FILLED, insert en outbox (settlement)
     *      en la MISMA transaccion -> atomicidad estado+evento.
     *      Si es CANCELLED, no se emite settlement (regla del dominio).
     */
    @Transactional
    public void apply(ExecutionReport er) {
        // 1. Lock pesimista sobre una orden EXISTENTE.
        java.util.Optional<Order> existing = orderRepository.findByIdForUpdate(er.numericOrderId());

        Order order;
        if (existing.isPresent()) {
            order = existing.get();
        } else {
            // No existe la orden todavia: el PDF garantiza que el broker
            // entrega los ER en orden de emision, y que toda orden
            // "siempre arranca con un NEW". Si el primer ER que vemos
            // para esta orden NO es NEW, es una violacion de esa
            // garantia (mensaje corrupto, perdido en el camino, fuera
            // de secuencia) - no la creamos con datos de un ER que no
            // deberia ser el fundacional. Ver DECISIONS.md: se trata
            // como error permanente, no se infiere un NEW implicito.
            if (!"NEW".equals(er.status())) {
                throw new com.maxcapital.orderprocessing.exception.PermanentProcessingException(
                    "Se recibio ER con status=%s para orden %d que no existe: se esperaba NEW como primer ER"
                        .formatted(er.status(), er.numericOrderId()));
            }
            order = orderRepository.save(createNewOrder(er));
            // save() aca es necesario: el ledger tiene FK hacia orders,
            // asi que la orden debe existir en DB antes del insert
            // idempotente del paso 2.
        }

        // 2. Insert idempotente en el ledger. 0 filas = ya se habia
        // aplicado este ER antes (reentrega) -> no tocar la orden.
        int inserted = ledgerRepository.insertIfNotExists(
            er.fixId(),
            er.numericOrderId(),
            er.status(),
            er.executionPrice(),
            er.executionNominalAmount(),
            er.secondaryTradeId(),
            er.operationNumber(),
            er.transactionTime()
        );

        if (inserted == 0) {
            log.info("ER duplicado ignorado: order={} secondaryTradeId={} operationNumber={}",
                er.numericOrderId(), er.secondaryTradeId(), er.operationNumber());
            return;
        }

        // 3. Update incremental: valida transicion contra el status
        // YA persistido (no sobreescribe ciegamente). Si el ER que
        // creo la orden es el mismo que el NEW inicial, esto vuelve
        // a aplicar NEW->NEW la primera vez - ver nota en createNewOrder.
        OrderStatus newStatus = OrderStatus.valueOf(er.status());
        order.applyExecutionReport(newStatus, er.leavesNominalAmount(), er.accumulativeNominalAmount());
        orderRepository.save(order);

        // 4. Settlement solo si llega a FILLED, misma transaccion (outbox).
        // CANCELLED no emite settlement (regla explicita del dominio).
        if (newStatus == OrderStatus.FILLED) {
            emitSettlementToOutbox(order);
        }
    }

    private Order createNewOrder(ExecutionReport er) {
        Order order = new Order();
        order.setNumericOrderId(er.numericOrderId());
        order.setTicker(er.ticker());
        order.setSide(er.side());
        order.setStatus(OrderStatus.NEW);
        order.setNominalAmounts(er.nominalAmounts());
        order.setLeavesNominalAmount(er.nominalAmounts());
        order.setAccumulativeNominalAmount(java.math.BigDecimal.ZERO);
        order.setExecutionsCount(0);
        return order;
    }

    private void emitSettlementToOutbox(Order order) {
        try {
            String payload = objectMapper.writeValueAsString(Map.of(
                "numericOrderId", order.getNumericOrderId(),
                "ticker", order.getTicker(),
                "status", order.getStatus().name(),
                "accumulativeNominalAmount", order.getAccumulativeNominalAmount()
            ));

            OutboxEvent event = OutboxEvent.builder()
                .numericOrderId(order.getNumericOrderId())
                .eventType("SETTLEMENT")
                .payload(payload)
                .published(false)
                .build();

            outboxRepository.save(event);
            // uq_outbox_order_settlement protege ante doble insert por
            // esta misma orden, en caso de que la logica de arriba
            // fallara y se intentara emitir el settlement dos veces.
        } catch (Exception e) {
            log.error("Error serializando settlement para orden {}", order.getNumericOrderId(), e);
            throw new IllegalStateException("No se pudo preparar el evento de settlement", e);
        }
    }
}
