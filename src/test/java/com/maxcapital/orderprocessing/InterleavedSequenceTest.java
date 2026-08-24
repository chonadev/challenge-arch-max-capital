package com.maxcapital.orderprocessing;

import com.maxcapital.orderprocessing.model.Order;
import com.maxcapital.orderprocessing.model.OrderStatus;
import com.maxcapital.orderprocessing.repository.ExecutionLedgerRepository;
import com.maxcapital.orderprocessing.repository.OrderRepository;
import com.maxcapital.orderprocessing.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

import static com.maxcapital.orderprocessing.ExecutionReportTestBuilder.er;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cubre la garantia central del PDF: "los ER de una misma orden deben
 * aplicarse en la secuencia en que fueron emitidos [...] entre ordenes
 * distintas no importa el orden relativo". Simula ER de dos ordenes
 * intercalados (A, B, A, B, A) y verifica que cada orden refleja
 * fielmente SU PROPIA secuencia, sin cruzarse con la otra.
 */
class InterleavedSequenceTest extends AbstractIntegrationTest {

    @Autowired
    private OrderProcessingService orderProcessingService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ExecutionLedgerRepository ledgerRepository;

    @Test
    void erIntercalados_deDosOrdenesDistintas_cadaOrdenReflejaSuPropiaSecuencia() {
        Long orderA = 30001L;
        Long orderB = 30002L;

        // Secuencia intercalada: A-NEW, B-NEW, A-PARTIAL, B-PARTIAL, A-FILLED, B-CANCELLED
        orderProcessingService.apply(
            er().numericOrderId(orderA).fixId(1L).status("NEW")
                .secondaryTradeId("ST-A1").operationNumber("OP-A1").build());

        orderProcessingService.apply(
            er().numericOrderId(orderB).fixId(2L).status("NEW")
                .secondaryTradeId("ST-B1").operationNumber("OP-B1").build());

        orderProcessingService.apply(
            er().numericOrderId(orderA).fixId(3L).status("PARTIALLY_FILLED")
                .leavesNominalAmount(BigDecimal.valueOf(600))
                .accumulativeNominalAmount(BigDecimal.valueOf(400))
                .executionNominalAmount(BigDecimal.valueOf(400))
                .secondaryTradeId("ST-A2").operationNumber("OP-A2").build());

        orderProcessingService.apply(
            er().numericOrderId(orderB).fixId(4L).status("PARTIALLY_FILLED")
                .leavesNominalAmount(BigDecimal.valueOf(700))
                .accumulativeNominalAmount(BigDecimal.valueOf(300))
                .executionNominalAmount(BigDecimal.valueOf(300))
                .secondaryTradeId("ST-B2").operationNumber("OP-B2").build());

        orderProcessingService.apply(
            er().numericOrderId(orderA).fixId(5L).status("FILLED")
                .leavesNominalAmount(BigDecimal.ZERO)
                .accumulativeNominalAmount(BigDecimal.valueOf(1000))
                .executionNominalAmount(BigDecimal.valueOf(600))
                .secondaryTradeId("ST-A3").operationNumber("OP-A3").build());

        orderProcessingService.apply(
            er().numericOrderId(orderB).fixId(6L).status("CANCELLED")
                .secondaryTradeId("ST-B3").operationNumber("OP-B3").build());

        // Orden A: NEW -> PARTIALLY_FILLED -> FILLED, 3 ejecuciones
        Order a = orderRepository.findById(orderA).orElseThrow();
        assertThat(a.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(a.getExecutionsCount()).isEqualTo(3);
        assertThat(a.getLeavesNominalAmount()).isEqualByComparingTo(BigDecimal.ZERO);

        // Orden B: NEW -> PARTIALLY_FILLED -> CANCELLED, 3 ejecuciones,
        // no llega a FILLED
        Order b = orderRepository.findById(orderB).orElseThrow();
        assertThat(b.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(b.getExecutionsCount()).isEqualTo(3);

        // el ledger de cada orden solo tiene SUS PROPIOS ER, en el
        // orden de insercion correcto (por id autoincremental)
        var ledgerA = ledgerRepository.findByNumericOrderIdOrderByIdAsc(orderA);
        assertThat(ledgerA).hasSize(3);
        assertThat(ledgerA.stream().map(l -> l.getStatusApplied().name()).toList())
            .containsExactly("NEW", "PARTIALLY_FILLED", "FILLED");

        var ledgerB = ledgerRepository.findByNumericOrderIdOrderByIdAsc(orderB);
        assertThat(ledgerB).hasSize(3);
        assertThat(ledgerB.stream().map(l -> l.getStatusApplied().name()).toList())
            .containsExactly("NEW", "PARTIALLY_FILLED", "CANCELLED");
    }
}
