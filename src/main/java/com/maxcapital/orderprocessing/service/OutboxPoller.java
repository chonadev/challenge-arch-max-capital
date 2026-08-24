package com.maxcapital.orderprocessing.service;

import com.maxcapital.orderprocessing.model.OutboxEvent;
import com.maxcapital.orderprocessing.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
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
 * FOR UPDATE SKIP LOCKED (en el repository): con las dos instancias
 * corriendo su propio poller en paralelo, cada una toma un subconjunto
 * de filas sin coordinacion explicita entre ellas ni pisarse.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic-settlement}")
    private String settlementTopic;

    @Value("${app.outbox.batch-size}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void pollAndPublish() {
        List<OutboxEvent> pending = fetchPendingBatch();
        if (!pending.isEmpty()) {
            log.info("OutboxPoller: {} eventos pendientes encontrados", pending.size());
        }

        for (OutboxEvent event : pending) {
            publishOne(event);
        }
    }

    /**
     * Lectura del batch en su propia transaccion corta: el lock de
     * SKIP LOCKED se toma y se libera aca (tras el SELECT), antes de
     * hacer las llamadas de red a Kafka - asi no mantenemos filas
     * bloqueadas mientras esperamos el ack del broker.
     */
    @Transactional
    protected List<OutboxEvent> fetchPendingBatch() {
        return outboxRepository.findPendingBatch(batchSize);
    }

    /**
     * Publica un evento y SOLO SI el broker confirma el ack, marca
     * published=true. Si el send falla o el proceso muere antes de
     * marcar, la fila sigue published=false y se vuelve a intentar
     * en el proximo ciclo - a costa de un posible reenvio duplicado,
     * que el consumidor downstream debe poder deduplicar por
     * numericOrderId (documentado en DECISIONS.md).
     */
    private void publishOne(OutboxEvent event) {
        try {
            kafkaTemplate.send(settlementTopic, String.valueOf(event.getNumericOrderId()),
                    event.getPayload())
                .get(); // espera confirmacion sincronica del broker

            markAsPublished(event.getId());
            log.info("Settlement publicado: outboxId={} numericOrderId={}",
                event.getId(), event.getNumericOrderId());

        } catch (Exception e) {
            log.error("Fallo publicando settlement outbox id={} numericOrderId={}. " +
                    "Se reintentara en el proximo ciclo del poller.",
                event.getId(), event.getNumericOrderId(), e);
            // no se marca published=true: queda pendiente para el
            // proximo poll, no se pierde
        }
    }

    @Transactional
    protected void markAsPublished(Long outboxId) {
        outboxRepository.findById(outboxId).ifPresent(event -> {
            event.setPublished(true);
            event.setPublishedAt(LocalDateTime.now());
            outboxRepository.save(event);
        });
    }
}
