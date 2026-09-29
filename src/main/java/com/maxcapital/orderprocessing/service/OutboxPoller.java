package com.maxcapital.orderprocessing.service;

import com.maxcapital.orderprocessing.model.OutboxEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Publica a Kafka los eventos de settlement pendientes en la tabla outbox.
 *
 * Por que un poller separado y no publicar directo desde
 * OrderProcessingService: el insert en outbox y el update de la orden
 * comparten transaccion (atomicidad DB-vs-DB). El publish a Kafka es
 * DB-vs-broker, no puede ser atomico con lo anterior - por eso se
 * delega a este proceso aparte, que puede reintentar sin riesgo de
 * dejar la orden en un estado inconsistente.
 *
 * Exclusion mutua entre las dos instancias: se resuelve con el CLAIM
 * (locked_at) de OutboxRepositoryService, no solo con el
 * FOR UPDATE SKIP LOCKED del SELECT. El SKIP LOCKED alcanza mientras
 * los locks estan tomados; el claim, ademas, queda escrito en la misma
 * transaccion del SELECT, asi que sigue siendo visible para el otro
 * poller aunque su SELECT arranque despues de que esta transaccion ya
 * commiteo. Sin ese segundo paso, dos pollers con SELECTs no solapados
 * leian la misma fila published=false y ambos la publicaban.
 *
 * Lo que este diseno NO garantiza:Exactly-once. Si el proceso muere
 * entre el ack del broker y el markAsPublished, el evento se republica
 * en el proximo ciclo. La deduplicacion por numericOrderId es
 * responsabilidad del consumidor downstream (SPEC.md G6).
 */
@Service
@ConditionalOnProperty(name = "app.outbox.poller-enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxRepositoryService outboxRepositoryService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic-settlement}")
    private String settlementTopic;

    @Value("${app.outbox.batch-size}")
    private int batchSize;

    @Value("${app.outbox.claim-stale-secs}")
    private int claimStaleSecs;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void pollAndPublish() {
        List<OutboxEvent> pending =
                outboxRepositoryService.claimPendingBatch(batchSize, claimStaleSecs);
        if (!pending.isEmpty()) {
            log.info("OutboxPoller: {} eventos pendientes encontrados", pending.size());
        }

        for (OutboxEvent event : pending) {
            publishOne(event);
        }
    }

    private void publishOne(OutboxEvent event) {
        try {
            kafkaTemplate.send(settlementTopic, String.valueOf(event.getNumericOrderId()),
                    event.getPayload())
                .get(); // espera confirmacion sincronica del broker

            outboxRepositoryService.markAsPublished(event.getId());
            log.info("Settlement publicado: outboxId={} numericOrderId={}",
                event.getId(), event.getNumericOrderId());

        } catch (Exception e) {
            log.error("Fallo publicando settlement outbox id={} numericOrderId={}. " +
                    "Se reintentara en el proximo ciclo del poller.",
                event.getId(), event.getNumericOrderId(), e);
            // no se marca published=true: queda pendiente para el
            // proximo poll, no se pierde. Se libera el claim para que el
            // reintento sea en el proximo ciclo y no al vencer el stale.
            releaseClaimQuietly(event.getId());
        }
    }

    private void releaseClaimQuietly(Long outboxId) {
        try {
            outboxRepositoryService.releaseClaim(outboxId);
        } catch (Exception releaseError) {
            // no enmascara el error original: el claim vence solo por
            // app.outbox.claim-stale-secs, asi que la fila se reintenta igual
            log.warn("No se pudo liberar el claim del outbox id={}. Reintento por " +
                    "vencimiento del claim en {}s.", outboxId, claimStaleSecs, releaseError);
        }
    }
}
