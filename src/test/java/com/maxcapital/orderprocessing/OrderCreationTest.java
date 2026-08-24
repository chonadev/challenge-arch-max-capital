package com.maxcapital.orderprocessing;

import com.maxcapital.orderprocessing.exception.PermanentProcessingException;
import com.maxcapital.orderprocessing.model.Order;
import com.maxcapital.orderprocessing.model.OrderStatus;
import com.maxcapital.orderprocessing.repository.OrderRepository;
import com.maxcapital.orderprocessing.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static com.maxcapital.orderprocessing.ExecutionReportTestBuilder.er;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cubre la invariante I1 de la spec: toda orden arranca con un ER
 * de status NEW. Un ER no-NEW para una orden inexistente es una
 * violacion de secuencia (spec D5).
 */
class OrderCreationTest extends AbstractIntegrationTest {

    @Autowired
    private OrderProcessingService orderProcessingService;

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void primerErEsNew_creaLaOrdenCorrectamente() {
        Long orderId = 50001L;

        var newEr = er().numericOrderId(orderId).fixId(1L).status("NEW")
                .secondaryTradeId("OCT-ST-A").operationNumber("OCT-OP-A").build();

        orderProcessingService.apply(newEr);

        Order order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.NEW);
        assertThat(order.getExecutionsCount()).isEqualTo(1);
    }

    @Test
    void primerErNoEsNew_paraOrdenInexistente_esRechazado() {
        Long orderId = 50002L;

        var partialEr = er().numericOrderId(orderId).fixId(1L).status("PARTIALLY_FILLED")
                .secondaryTradeId("OCT-ST-B").operationNumber("OCT-OP-B").build();

        assertThatThrownBy(() -> orderProcessingService.apply(partialEr))
                .isInstanceOf(PermanentProcessingException.class);

        assertThat(orderRepository.findById(orderId)).isEmpty();
    }
}