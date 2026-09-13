package com.maxcapital.orderprocessing.kafka;

import com.maxcapital.orderprocessing.dto.ExecutionReport;
import com.maxcapital.orderprocessing.exception.InvalidStateTransitionException;
import com.maxcapital.orderprocessing.exception.PermanentProcessingException;
import com.maxcapital.orderprocessing.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Consume er.raw. Gestion automatica de reintentos mediante @RetryableTopic:
 *   (a) exito -> procesado correctamente
 *   (b) error permanente (PermanentProcessingException, InvalidStateTransitionException) ->
 *       se excluye de reintentos y va directamente a er.raw.dlq
 *   (c) error transitorio / inesperado -> se reintenta hasta 3 veces con backoff
 *       y si se agotan, va a er.raw.dlq
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ExecutionReportListener {

    private final OrderProcessingService orderProcessingService;

    @RetryableTopic(
        attempts = "3",
        backoff = @Backoff(delay = 1000, multiplier = 2.0),
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltTopicSuffix = ".dlq",
        exclude = {PermanentProcessingException.class, InvalidStateTransitionException.class}
    )
    @KafkaListener(topics = "${app.kafka.topic-er-raw}")
    public void onExecutionReport(ConsumerRecord<String, ExecutionReport> record) {
        ExecutionReport er = record.value();
        try {
            validate(er);
            orderProcessingService.apply(er);
        } catch (PermanentProcessingException | InvalidStateTransitionException e) {
            log.error("Error permanente procesando ER numericOrderId={}, key={}: {}",
                er != null ? er.numericOrderId() : "null", record.key(), e.getMessage());
            throw e;
        }
    }

    @DltHandler
    public void handleDlt(ConsumerRecord<String, ExecutionReport> record,
                          @Header(required = false, name = KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {
        log.error("FALLO CRITICO / DLQ: Mensaje agotó reintentos o fue derivado a DLQ. key={}, error={}",
            record.key(), errorMessage);
    }

    private void validate(ExecutionReport er) {
        if (er == null || er.numericOrderId() == null || er.fixId() == null
            || er.status() == null || er.secondaryTradeId() == null
            || er.operationNumber() == null) {
            throw new PermanentProcessingException("ER invalido: faltan campos obligatorios");
        }
    }
}
