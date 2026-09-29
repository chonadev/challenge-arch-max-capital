package com.maxcapital.orderprocessing.repository;

import com.maxcapital.orderprocessing.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Seleccion de filas reclamables por este ciclo del poller.
     *
     * SKIP LOCKED y el filtro de locked_at hacen cosas distintas y las
     * dos hacen falta:
     *
     * - SKIP LOCKED reparte las filas cuando dos pollers se pisan EN EL
     *   MISMO instante: el que pierde la carrera no espera el lock.
     * - El filtro de locked_at es el que sostiene la exclusion. El lock
     *   de lectura se libera al commitear, pero el claim sigue escrito:
     *   un poller cuyo SELECT arranque mas tarde ve la fila ya tomada y
     *   la saltea. Sin esto, dos SELECTs no solapados leen la misma fila
     *   y ambos la publican.
     *
     * Solo vuelve a ser candidata si el claim quedo viejo (poller
     * caido a mitad de publish).
     *
     * Este metodo NO se llama solo - se usa dentro de claimPendingBatch
     * (OutboxRepositoryService), que mantiene la transaccion abierta y
     * escribe el locked_at antes del commit.
     */
    @Query(value = """
        SELECT id FROM outbox
        WHERE published = false
          AND (locked_at IS NULL
               OR locked_at < now() - make_interval(secs => CAST(:staleSeconds AS double precision)))
        ORDER BY id ASC
        LIMIT :batchSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Long> findClaimableIds(@Param("batchSize") int batchSize,
                                @Param("staleSeconds") int staleSeconds);

    /**
     * Sella el claim. Se ejecuta dentro de la transaccion de
     * claimPendingBatch, junto con el SELECT que tomo los locks: al
     * commit, la reserva queda durable y sobrevive a que la transaccion
     * termine y los locks se liberen.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE outbox SET locked_at = :lockedAt WHERE id IN (:ids)", nativeQuery = true)
    void markClaimed(@Param("ids") List<Long> ids, @Param("lockedAt") LocalDateTime lockedAt);

    /**
     * Cierra el ciclo en el camino feliz: published=true, published_at,
     * y se libera locked_at (la fila ya no es candidata de todas formas,
     * pero dejarla sin claim mantiene la tabla consistente).
     */
    @Modifying
    @Query("UPDATE OutboxEvent o SET o.published = true, o.publishedAt = :publishedAt, o.lockedAt = NULL " +
           "WHERE o.id = :id")
    void markAsPublished(@Param("id") Long id, @Param("publishedAt") LocalDateTime publishedAt);

    /**
     * Devuelve la fila al pool en el proximo ciclo en vez de esperar a
     * que venza el claim. Se usa cuando el publish falla: no perdemos
     * tiempo de retry por un timeout que solo aplica a polleres caidos.
     */
    @Modifying
    @Query("UPDATE OutboxEvent o SET o.lockedAt = NULL WHERE o.id = :id AND o.published = false")
    void releaseClaim(@Param("id") Long id);
}
