package com.maxcapital.orderprocessing.repository;

import com.maxcapital.orderprocessing.model.ExecutionLedger;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public interface ExecutionLedgerRepository extends JpaRepository<ExecutionLedger, Long> {

    /**
     * Ledger completo de una orden en orden de insercion (id asc).
     * Usado por el flujo de consulta paginado (via el service) y por
     * los tests de integracion que verifican la secuencia completa.
     */
    List<ExecutionLedger> findByNumericOrderIdOrderByIdAsc(Long numericOrderId);

    /**
     * Ledger de una orden, paginado. El ORDER BY por id NO se deriva
     * aqui: el sort lo fija el servicio (id ascendente = orden de
     * insercion), para que el contrato no dependa del llamador.
     */
    Page<ExecutionLedger> findByNumericOrderId(Long numericOrderId, Pageable pageable);

    boolean existsBySecondaryTradeIdAndOperationNumber(String secondaryTradeId, String operationNumber);

    /**
     * Insert idempotente via ON CONFLICT DO NOTHING sobre el
     * UNIQUE(secondary_trade_id, operation_number) - la clave de dedup
     * que el PDF marca explicitamente como "identidad de la ejecucion".
     * Devuelve la cantidad de filas insertadas: 0 significa que el ER ya
     * habia sido aplicado antes (reentrega), 1 significa que es nuevo.
     * Este es el mecanismo real de idempotencia, no un SELECT previo.
     */
    @Modifying
    @Query(value = """
        INSERT INTO execution_ledger
            (fix_id, numeric_order_id, status_applied, execution_price,
             execution_nominal_amount, secondary_trade_id, operation_number,
             transaction_time, applied_at)
        VALUES
            (:fixId, :numericOrderId, :statusApplied, :executionPrice,
             :executionNominalAmount, :secondaryTradeId, :operationNumber,
             :transactionTime, now())
        ON CONFLICT (secondary_trade_id, operation_number) DO NOTHING
        """, nativeQuery = true)
    int insertIfNotExists(
        @Param("fixId") Long fixId,
        @Param("numericOrderId") Long numericOrderId,
        @Param("statusApplied") String statusApplied,
        @Param("executionPrice") BigDecimal executionPrice,
        @Param("executionNominalAmount") BigDecimal executionNominalAmount,
        @Param("secondaryTradeId") String secondaryTradeId,
        @Param("operationNumber") String operationNumber,
        @Param("transactionTime") LocalDateTime transactionTime
    );
}
