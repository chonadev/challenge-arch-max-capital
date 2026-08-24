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
 * Cubre el requisito central de idempotencia del PDF: "un ER duplicado
 * o reentregado no debe corromper el estado" y "la identidad de un
 * duplicado es el ER individual (secondaryTradeId+operationNumber),
 * no la orden".
 */
class IdempotencyTest extends AbstractIntegrationTest {

    @Autowired
    private OrderProcessingService orderProcessingService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ExecutionLedgerRepository ledgerRepository;

    @Test
    void erDuplicado_noDuplicaLedger_niIncrementaExecutionsCountDosVeces() {
        Long orderId = 10001L;

        var newEr = er().numericOrderId(orderId).fixId(1L).status("NEW")
            .secondaryTradeId("IDT-ST-A").operationNumber("IDT-OP-A").build();

        var partialEr = er().numericOrderId(orderId).fixId(2L).status("PARTIALLY_FILLED")
            .leavesNominalAmount(BigDecimal.valueOf(600))
            .accumulativeNominalAmount(BigDecimal.valueOf(400))
            .executionNominalAmount(BigDecimal.valueOf(400))
            .secondaryTradeId("IDT-ST-B").operationNumber("IDT-OP-B").build();

        orderProcessingService.apply(newEr);
        orderProcessingService.apply(partialEr);

        // reentrega: mismo secondaryTradeId+operationNumber que partialEr,
        // distinto fixId a proposito para probar que el dedup NO se basa
        // en fixId sino en (secondaryTradeId, operationNumber)
        var partialErReentregado = er().numericOrderId(orderId).fixId(999L).status("PARTIALLY_FILLED")
            .leavesNominalAmount(BigDecimal.valueOf(600))
            .accumulativeNominalAmount(BigDecimal.valueOf(400))
            .executionNominalAmount(BigDecimal.valueOf(400))
            .secondaryTradeId("IDT-ST-B").operationNumber("IDT-OP-B").build();

        orderProcessingService.apply(partialErReentregado);

        Order order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(order.getExecutionsCount()).isEqualTo(2); // no 3
        assertThat(order.getLeavesNominalAmount()).isEqualByComparingTo(BigDecimal.valueOf(600));

        assertThat(ledgerRepository.findByNumericOrderIdOrderByIdAsc(orderId)).hasSize(2);
    }

    @Test
    void erNuevo_conMismoFixIdDeUnoAnterior_peroDistintaClaveDeDedup_siSeAplica() {
        // Verifica explicitamente que fixId NO es la clave de dedup:
        // dos ER con el MISMO fixId pero distinto (secondaryTradeId,
        // operationNumber) deben tratarse como eventos DISTINTOS.
        Long orderId = 10002L;

        var newEr = er().numericOrderId(orderId).fixId(5L).status("NEW")
            .secondaryTradeId("IDT-ST-X").operationNumber("IDT-OP-X").build();

        var partialEr = er().numericOrderId(orderId).fixId(5L).status("PARTIALLY_FILLED") // mismo fixId=5
            .leavesNominalAmount(BigDecimal.valueOf(500))
            .accumulativeNominalAmount(BigDecimal.valueOf(500))
            .executionNominalAmount(BigDecimal.valueOf(500))
            .secondaryTradeId("IDT-ST-Y").operationNumber("IDT-OP-Y").build(); // distinta clave de dedup

        orderProcessingService.apply(newEr);
        orderProcessingService.apply(partialEr);

        Order order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getExecutionsCount()).isEqualTo(2); // ambos se aplicaron
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
    }
}
