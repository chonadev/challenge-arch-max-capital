package com.maxcapital.orderprocessing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cubre spec S4.8: ejercita ExecutionReportListener real (no el
 * servicio directamente), publicando mensajes crudos a er.raw y
 * verificando que los errores permanentes terminan en er.raw.dlq
 * sin bloquear el flujo ni perderse en silencio.
 */
class ExecutionReportListenerDlqTest extends AbstractIntegrationTest {

    @Autowired
    private com.maxcapital.orderprocessing.repository.OrderRepository orderRepository;

    @Test
    void erConCamposFaltantes_terminaEnDlq_noCreaOrden() throws Exception {
        Long orderId = 60001L;
        String json = """
            {"fixId":1,"numericOrderId":%d,"marketOrderId":"M1","ticker":"TEST",
             "side":"BUY","securityType":"COMMON_STOCK","status":"NEW",
             "orderPrice":100,"nominalAmounts":1000,"leavesNominalAmount":1000,
             "accumulativeNominalAmount":0,"executionNominalAmount":0,
             "executionPrice":null,"avgPrice":null,
             "secondaryTradeId":null,"operationNumber":null,
             "transactionTime":"2026-08-23T12:00:00"}
            """.formatted(orderId);

        publishRaw("er.raw", String.valueOf(orderId), json);

        ConsumerRecord<String, String> dlqRecord = consumeFromDlqForOrder("er.raw.dlq", String.valueOf(orderId));

        assertThat(dlqRecord.value()).contains(String.valueOf(orderId));
        assertThat(orderRepository.findById(orderId)).isEmpty();
    }

    private void publishRaw(String topic, String key, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value));
            producer.flush();
        }
    }

    private ConsumerRecord<String, String> consumeFromDlqForOrder(String topic, String expectedKey) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 15000;

            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (expectedKey.equals(record.key())) {
                        return record;
                    }
                }
            }
            throw new AssertionError("No se encontro mensaje con key=" + expectedKey
                    + " en " + topic + " dentro del timeout");
        }
    }

    @Test
    void erConTransicionInvalida_terminaEnDlq_noModificaOrden() throws Exception {
        Long orderId = 60002L;

        // 1. Llevar la orden a FILLED via el listener real (no el
        // servicio directo) para que el escenario sea end-to-end
        publishRaw("er.raw", String.valueOf(orderId), erJson(orderId, 1,
                "NEW", "0", "1000", "0", "ELT-ST-A", "ELT-OP-A"));
        publishRaw("er.raw", String.valueOf(orderId), erJson(orderId, 2,
                "FILLED", "0", "1000", "1000", "ELT-ST-B", "ELT-OP-B"));

        // esperar a que ambos ER se procesen y la orden quede FILLED
        waitUntilOrderReachesStatus(orderId, "FILLED", 15000);

        // 2. ER legitimamente nuevo (clave de dedup nunca vista) pero
        // la orden ya esta en estado terminal
        publishRaw("er.raw", String.valueOf(orderId), erJson(orderId, 3,
                "PARTIALLY_FILLED", "500", "500", "500", "ELT-ST-C", "ELT-OP-C"));

        ConsumerRecord<String, String> dlqRecord = consumeFromDlqForOrder("er.raw.dlq", String.valueOf(orderId));

        assertThat(dlqRecord.value()).contains(String.valueOf(orderId));

        var order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus().name()).isEqualTo("FILLED");
        assertThat(order.getExecutionsCount()).isEqualTo(2);
    }

    private String erJson(Long orderId, int fixId, String status, String leaves,
                          String nominal, String accumulative,
                          String secondaryTradeId, String operationNumber) {
        return """
            {"fixId":%d,"numericOrderId":%d,"marketOrderId":"M%d","ticker":"TEST",
             "side":"BUY","securityType":"COMMON_STOCK","status":"%s",
             "orderPrice":100,"nominalAmounts":%s,"leavesNominalAmount":%s,
             "accumulativeNominalAmount":%s,"executionNominalAmount":0,
             "executionPrice":null,"avgPrice":null,
             "secondaryTradeId":"%s","operationNumber":"%s",
             "transactionTime":"2026-08-23T12:00:00"}
            """.formatted(fixId, orderId, orderId, status, nominal, leaves,
                accumulative, secondaryTradeId, operationNumber);
    }

    private void waitUntilOrderReachesStatus(Long orderId, String expectedStatus, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var order = orderRepository.findById(orderId);
            if (order.isPresent() && order.get().getStatus().name().equals(expectedStatus)) {
                return;
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("Orden " + orderId + " no alcanzo status " + expectedStatus
                + " dentro del timeout");
    }
}