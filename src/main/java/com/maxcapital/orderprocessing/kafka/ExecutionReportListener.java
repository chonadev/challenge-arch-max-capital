package com.maxcapital.orderprocessing.kafka;

import com.maxcapital.orderprocessing.dto.ExecutionReport;
import com.maxcapital.orderprocessing.exception.InvalidStateTransitionException;
import com.maxcapital.orderprocessing.exception.PermanentProcessingException;
import com.maxcapital.orderprocessing.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consume er.raw. Ack manual: el offset solo se confirma cuando el
 * procesamiento termina de una de dos formas validas:
 *   (a) exito -> ack normal
 *   (b) error permanente -> se publica a er.raw.dlq y se hace ack
 *       (el mensaje no se pierde: queda visible en la DLQ; y no
 *       bloquea el flujo de esa orden ni de las demas)
 *
 * Si ocurre un error TRANSITORIO, no se hace ack: la excepcion se
 * propaga, Kafka no avanza el offset, y al reintentar el consumo
 * (mismo poll o tras restart) el mensaje se vuelve a entregar.
 * Esto es deliberadamente simple (retry mediante no-ack + reentrega
 * natural de Kafka) en vez de @RetryableTopic con backoff configurable,
 * como alcance consciente para el tiempo del challenge - ver DECISIONS.md.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ExecutionReportListener {

    private final OrderProcessingService orderProcessingService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic-er-dlq}")
    private String dlqTopic;

    @KafkaListener(topics = "${app.kafka.topic-er-raw}")
    public void onExecutionReport(ConsumerRecord<String, ExecutionReport> record,
                                   Acknowledgment ack) {
        ExecutionReport er = record.value();
        try {
            validate(er);
            orderProcessingService.apply(er);
            ack.acknowledge();

        } catch (PermanentProcessingException | InvalidStateTransitionException e) {
            // Ambas son errores permanentes: un ER invalido o un ER
            // legitimo que llega tarde sobre una orden ya terminal.
            // Ninguna se resuelve reintentando, asi que van a DLQ en
            // vez de bloquear indefinidamente el flujo de esta orden
            // (y, por particion, el de las demas ordenes detras de ella).
            log.error("Error permanente procesando ER numericOrderId={}, key={}: {}",
                er != null ? er.numericOrderId() : "null", record.key(), e.getMessage());
            publishToDlq(record, e.getMessage());
            ack.acknowledge(); // se descarta del topic principal, pero
                                // queda en DLQ - no es descarte silencioso

        }
        // Cualquier otra excepcion (fallo transitorio: timeout de DB,
        // conexion caida, etc.) se propaga sin catch: no hay ack, y el
        // mensaje queda disponible para reintento. No se distingue aca
        // de forma mas fina (ej: cuantos reintentos antes de mandar a
        // DLQ tambien) - alcance documentado en DECISIONS.md.
    }

    private void publishToDlq(ConsumerRecord<String, ExecutionReport> record, String reason) {
        try {
            kafkaTemplate.send(dlqTopic, record.key(), record.value());
        } catch (Exception sendError) {
            // si ni siquiera se puede publicar a la DLQ, lo dejamos en
            // logs con maxima severidad - este es el unico caso donde
            // el mensaje podria perderse, y queda explicito, no silencioso.
            log.error("FALLO CRITICO: no se pudo publicar a DLQ. key={} reason={}",
                record.key(), reason, sendError);
        }
    }

    private void validate(ExecutionReport er) {
        if (er == null || er.numericOrderId() == null || er.fixId() == null
            || er.status() == null || er.secondaryTradeId() == null
            || er.operationNumber() == null) {
            throw new PermanentProcessingException("ER invalido: faltan campos obligatorios");
        }
    }
}
