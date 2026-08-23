package com.maxcapital.orderprocessing.repository;

import com.maxcapital.orderprocessing.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * FOR UPDATE SKIP LOCKED: si las dos instancias corren su propio poller
     * en paralelo, cada una toma un subconjunto de filas sin pisarse,
     * sin necesidad de coordinacion explicita entre ellas.
     */
    @Query(value = """
        SELECT * FROM outbox
        WHERE published = false
        ORDER BY id ASC
        LIMIT :batchSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<OutboxEvent> findPendingBatch(@Param("batchSize") int batchSize);
}
