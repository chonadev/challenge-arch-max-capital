package com.maxcapital.orderprocessing;

import com.maxcapital.orderprocessing.model.OutboxEvent;
import com.maxcapital.orderprocessing.repository.OutboxRepository;
import com.maxcapital.orderprocessing.service.OrderProcessingService;
import com.maxcapital.orderprocessing.service.OutboxRepositoryService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;

import static com.maxcapital.orderprocessing.ExecutionReportTestBuilder.er;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cubre la exclusion mutua entre los dos pollers que corren en paralelo
 * (una instancia cada uno).
 *
 * La ventana que estos tests cubren: con la lectura sola
 * (SELECT ... FOR UPDATE SKIP LOCKED) el lock se libera al cerrar la
 * transaccion de lectura, pero el publish a Kafka y el markAsPublished
 * pasan despues. Dos pollers cuyos SELECT no se solapan leian la misma
 * fila published=false y la publicaban los dos. El claim (locked_at,
 * escrito en la misma transaccion que el SELECT) cierra esa ventana.
 *
 * El poller esta apagado en el perfil de test (app.outbox.poller-enabled)
 * porque compiria las mismas filas y las marcaria published antes de que
 * estas pruebas puedan reclamarlas.
 */
class OutboxClaimTest extends AbstractIntegrationTest {

    private static final int STALE_SECS = 60;
    private static final int BATCH = 1000;

    @Autowired
    private OrderProcessingService orderProcessingService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private OutboxRepositoryService outboxRepositoryService;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Suelta todos los claims. Sin esto cada test deja filas reclamadas
     * para siempre y el siguiente no encuentra nada que reclamar
     * (fallo dependiente del orden, no del comportamiento).
     */
    @AfterEach
    void soltarClaims() {
        transactionTemplate.executeWithoutResult(status ->
                entityManager.createNativeQuery(
                        "UPDATE outbox SET locked_at = NULL WHERE published = false")
                        .executeUpdate());
    }

    @Test
    void claim_devuelveLaFilaYLeSellaElClaim() {
        Long outboxId = crearOrdenFilled(70001L);

        List<OutboxEvent> claimed = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        assertThat(claimed).extracting(OutboxEvent::getId).contains(outboxId);
        assertThat(outboxRepository.findById(outboxId).orElseThrow().getLockedAt())
                .isNotNull();
    }

    @Test
    void segundoClaim_noVeLoQueElOtroPollerYaReclamo() {
        Long outboxId = crearOrdenFilled(70002L);

        List<OutboxEvent> primerClaim = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);
        // Segundo poller: su SELECT arranca DESPUES de que el primero ya
        // commiteo (locks liberados, pero el claim sigue escrito).
        List<OutboxEvent> segundoClaim = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        assertThat(primerClaim).extracting(OutboxEvent::getId).contains(outboxId);
        assertThat(segundoClaim).extracting(OutboxEvent::getId).doesNotContain(outboxId);
    }

    @Test
    void claimVencido_vuelveASerReclamable() {
        Long outboxId = crearOrdenFilled(70003L);
        outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        // Simula un poller que toma el claim y se cae sin publicar.
        // Pasado el umbral de staleness, la fila vuelve al pool.
        envejecerClaim(outboxId, 10);

        List<OutboxEvent> claim = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        assertThat(claim).extracting(OutboxEvent::getId).contains(outboxId);
    }

    @Test
    void claimVigente_noVuelveASerReclamable() {
        Long outboxId = crearOrdenFilled(70004L);
        outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        // Mismo escenario pero dentro del umbral: la fila sigue tomada.
        envejecerClaim(outboxId, 0);

        List<OutboxEvent> claim = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        assertThat(claim).extracting(OutboxEvent::getId).doesNotContain(outboxId);
    }

    @Test
    void releaseClaim_devuelveLaFilaAlPoolSinEsperarElVencimiento() {
        Long outboxId = crearOrdenFilled(70005L);
        outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        // Camino del publish fallido: se suelta el claim para reintentar
        // en el proximo ciclo, no al vencer el stale.
        outboxRepositoryService.releaseClaim(outboxId);

        assertThat(outboxRepository.findById(outboxId).orElseThrow().getLockedAt()).isNull();
        List<OutboxEvent> claim = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);
        assertThat(claim).extracting(OutboxEvent::getId).contains(outboxId);
    }

    @Test
    void markAsPublished_sacaLaFilaDelPool() {
        Long outboxId = crearOrdenFilled(70006L);
        outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);

        outboxRepositoryService.markAsPublished(outboxId);

        OutboxEvent published = outboxRepository.findById(outboxId).orElseThrow();
        assertThat(published.getPublished()).isTrue();
        assertThat(published.getPublishedAt()).isNotNull();
        assertThat(published.getLockedAt()).isNull();

        List<OutboxEvent> claim = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS);
        assertThat(claim).extracting(OutboxEvent::getId).doesNotContain(outboxId);
    }

    @Test
    void claim_devuelveElBatchOrdenadoPorId() {
        Long primeraFila = crearOrdenFilled(70007L);
        Long segundaFila = crearOrdenFilled(70008L);

        List<Long> ids = outboxRepositoryService.claimPendingBatch(BATCH, STALE_SECS)
                .stream().map(OutboxEvent::getId).toList();

        assertThat(ids).contains(primeraFila, segundaFila);
        assertThat(ids).isSorted();
    }

    private Long crearOrdenFilled(Long orderId) {
        orderProcessingService.apply(er().numericOrderId(orderId).fixId(1L).status("NEW")
                .secondaryTradeId("CLM-ST-" + orderId).operationNumber("CLM-OP-N-" + orderId)
                .build());
        orderProcessingService.apply(er().numericOrderId(orderId).fixId(2L).status("FILLED")
                .leavesNominalAmount(BigDecimal.ZERO)
                .accumulativeNominalAmount(BigDecimal.valueOf(1000))
                .executionNominalAmount(BigDecimal.valueOf(1000))
                .secondaryTradeId("CLM-ST-" + orderId).operationNumber("CLM-OP-F-" + orderId)
                .build());

        return outboxRepository.findAll().stream()
                .filter(o -> o.getNumericOrderId().equals(orderId))
                .findFirst()
                .orElseThrow()
                .getId();
    }

    private void envejecerClaim(Long outboxId, int minutes) {
        transactionTemplate.executeWithoutResult(status ->
                entityManager.createNativeQuery(
                        "UPDATE outbox SET locked_at = now() - make_interval(mins => :mins) WHERE id = :id")
                        .setParameter("mins", minutes)
                        .setParameter("id", outboxId)
                        .executeUpdate());
        entityManager.clear();
    }
}
