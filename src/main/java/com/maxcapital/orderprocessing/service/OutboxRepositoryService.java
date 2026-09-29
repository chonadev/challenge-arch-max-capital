package com.maxcapital.orderprocessing.service;

import com.maxcapital.orderprocessing.model.OutboxEvent;
import com.maxcapital.orderprocessing.repository.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

/**
 * Bean separado de OutboxPoller a proposito: las transacciones se abren
 * por proxy, asi que los metodos transaccionales deben vivir en OTRA
 * clase para que @Transactional sea efectivo.
 */
@Service
public class OutboxRepositoryService {

    private final OutboxRepository outboxRepository;

    public OutboxRepositoryService(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    /**
     * Reclama un batch de eventos pendientes de forma atomica.
     *
     * El SELECT con FOR UPDATE SKIP LOCKED y el UPDATE que escribe
     * locked_at ocurren en la MISMA transaccion. Por eso el claim queda
     * seteado en el momento del commit y no despues: cuando la
     * transaccion termina y los locks se liberan, la fila ya esta
     * marcada como tomada y ningun otro poller la va a elegir.
     *
     * Esto es lo que cierra la ventana entre "SELECT" y "markAsPublished":
     * antes, dos pollers con SELECTs no solapados leian la misma fila
     * published=false y la publicaban los dos.
     */
    @Transactional
    public List<OutboxEvent> claimPendingBatch(int batchSize, int staleSeconds) {
        List<Long> ids = outboxRepository.findClaimableIds(batchSize, staleSeconds);
        if (ids.isEmpty()) {
            return List.of();
        }
        outboxRepository.markClaimed(ids, LocalDateTime.now());
        // findAllById no garantiza orden, y el caller asume el mismo
        // criterio que el ORDER BY de la seleccion.
        return outboxRepository.findAllById(ids).stream()
                .sorted(Comparator.comparing(OutboxEvent::getId))
                .toList();
    }

    @Transactional
    public void markAsPublished(Long outboxId) {
        outboxRepository.markAsPublished(outboxId, LocalDateTime.now());
    }

    @Transactional
    public void releaseClaim(Long outboxId) {
        outboxRepository.releaseClaim(outboxId);
    }
}
