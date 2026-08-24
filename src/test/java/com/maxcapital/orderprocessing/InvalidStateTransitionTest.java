package com.maxcapital.orderprocessing;

import com.maxcapital.orderprocessing.exception.InvalidStateTransitionException;
import com.maxcapital.orderprocessing.model.Order;
import com.maxcapital.orderprocessing.model.OrderStatus;
import com.maxcapital.orderprocessing.repository.OrderRepository;
import com.maxcapital.orderprocessing.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

import static com.maxcapital.orderprocessing.ExecutionReportTestBuilder.er;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cubre el requisito del PDF: "un ER no puede aplicarse sobre una
 * orden ya terminal (FILLED / CANCELLED)". Distinto de idempotencia:
 * aca el ER es NUEVO (clave de dedup nunca vista), pero invalido para
 * el estado actual de la orden.
 */
class InvalidStateTransitionTest extends AbstractIntegrationTest {

    @Autowired
    private OrderProcessingService orderProcessingService;

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void erSobreOrdenYaFilled_esRechazado_yNoModificaLaOrden() {
        Long orderId = 20001L;

        var newEr = er().numericOrderId(orderId).fixId(1L).status("NEW")
            .secondaryTradeId("ISTT-ST-A").operationNumber("ISTT-OP-A").build();
        var filledEr = er().numericOrderId(orderId).fixId(2L).status("FILLED")
            .leavesNominalAmount(BigDecimal.ZERO)
            .accumulativeNominalAmount(BigDecimal.valueOf(1000))
            .executionNominalAmount(BigDecimal.valueOf(1000))
            .secondaryTradeId("ISTT-ST-B").operationNumber("ISTT-OP-B").build();

        orderProcessingService.apply(newEr);
        orderProcessingService.apply(filledEr);

        // ER legitimamente nuevo (clave de dedup nunca vista), pero
        // la orden ya esta en estado terminal
        var lateEr = er().numericOrderId(orderId).fixId(3L).status("PARTIALLY_FILLED")
            .leavesNominalAmount(BigDecimal.valueOf(200))
            .accumulativeNominalAmount(BigDecimal.valueOf(800))
            .executionNominalAmount(BigDecimal.valueOf(800))
            .secondaryTradeId("ISTT-ST-C").operationNumber("ISTT-OP-C").build();

        assertThatThrownBy(() -> orderProcessingService.apply(lateEr))
            .isInstanceOf(InvalidStateTransitionException.class);

        // la orden no debe haberse tocado por el intento fallido
        Order order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getExecutionsCount()).isEqualTo(2); // NEW + FILLED, no 3
    }

    @Test
    void erSobreOrdenCancelled_esRechazado() {
        Long orderId = 20002L;

        var newEr = er().numericOrderId(orderId).fixId(10L).status("NEW")
            .secondaryTradeId("ISTT-ST-D").operationNumber("ISTT-OP-D").build();
        var cancelledEr = er().numericOrderId(orderId).fixId(11L).status("CANCELLED")
            .secondaryTradeId("ISTT-ST-E").operationNumber("ISTT-OP-E").build();

        orderProcessingService.apply(newEr);
        orderProcessingService.apply(cancelledEr);

        var lateEr = er().numericOrderId(orderId).fixId(12L).status("PARTIALLY_FILLED")
            .secondaryTradeId("ISTT-ST-F").operationNumber("ISTT-OP-F").build();

        assertThatThrownBy(() -> orderProcessingService.apply(lateEr))
            .isInstanceOf(InvalidStateTransitionException.class);
    }
}
