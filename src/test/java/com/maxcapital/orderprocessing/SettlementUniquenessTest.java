package com.maxcapital.orderprocessing;

import com.maxcapital.orderprocessing.model.OutboxEvent;
import com.maxcapital.orderprocessing.repository.OutboxRepository;
import com.maxcapital.orderprocessing.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

import static com.maxcapital.orderprocessing.ExecutionReportTestBuilder.er;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cubre el requisito del PDF: el settlement debe llegar exactamente
 * una vez por orden downstream, "aun cuando haya reentregas de ER
 * [...] (tipicamente via idempotencia / outbox / dedup)". Este test
 * verifica la capa de outbox (insert unico), no el publish a Kafka
 * en si (cubierto aparte por OutboxPoller, ver DECISIONS.md sobre
 * alcance de testing).
 */
class SettlementUniquenessTest extends AbstractIntegrationTest {

    @Autowired
    private OrderProcessingService orderProcessingService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Test
    void erQueCompletaOrden_reentregado_noDuplicaFilaEnOutbox() {
        Long orderId = 40001L;

        var newEr = er().numericOrderId(orderId).fixId(1L).status("NEW")
            .secondaryTradeId("SUT-ST-A").operationNumber("SUT-OP-A").build();

        var filledEr = er().numericOrderId(orderId).fixId(2L).status("FILLED")
            .leavesNominalAmount(BigDecimal.ZERO)
            .accumulativeNominalAmount(BigDecimal.valueOf(1000))
            .executionNominalAmount(BigDecimal.valueOf(1000))
            .secondaryTradeId("SUT-ST-B").operationNumber("SUT-OP-B").build();

        orderProcessingService.apply(newEr);
        orderProcessingService.apply(filledEr);

        // reentrega exacta del mismo ER (misma clave de dedup) que
        // disparo el FILLED - simula la reentrega del broker
        orderProcessingService.apply(filledEr);

        var outboxRows = outboxRepository.findAll().stream()
            .filter(o -> o.getNumericOrderId().equals(orderId))
            .toList();

        assertThat(outboxRows).hasSize(1);
        assertThat(outboxRows.get(0).getEventType()).isEqualTo("SETTLEMENT");
    }

    @Test
    void ordenCancelada_noGeneraFilaEnOutbox() {
        Long orderId = 40002L;

        var newEr = er().numericOrderId(orderId).fixId(10L).status("NEW")
            .secondaryTradeId("SUT-ST-C").operationNumber("SUT-OP-C").build();
        var cancelledEr = er().numericOrderId(orderId).fixId(11L).status("CANCELLED")
            .secondaryTradeId("SUT-ST-D").operationNumber("SUT-OP-D").build();

        orderProcessingService.apply(newEr);
        orderProcessingService.apply(cancelledEr);

        var outboxRows = outboxRepository.findAll().stream()
            .filter(o -> o.getNumericOrderId().equals(orderId))
            .toList();

        assertThat(outboxRows).isEmpty();
    }
}
