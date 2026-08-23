package com.maxcapital.orderprocessing.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "execution_ledger")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExecutionLedger {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "fix_id", nullable = false)
    private Long fixId;

    @Column(name = "numeric_order_id", nullable = false)
    private Long numericOrderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status_applied", nullable = false, length = 20)
    private OrderStatus statusApplied;

    @Column(name = "execution_price", precision = 18, scale = 4)
    private BigDecimal executionPrice;

    @Column(name = "execution_nominal_amount", precision = 18, scale = 4)
    private BigDecimal executionNominalAmount;

    /**
     * Junto con operationNumber, forma la clave de dedup del ER individual.
     * El PDF marca ambos campos explicitamente como "para dedup".
     */
    @Column(name = "secondary_trade_id", nullable = false, length = 64)
    private String secondaryTradeId;

    @Column(name = "operation_number", nullable = false, length = 64)
    private String operationNumber;

    @Column(name = "transaction_time")
    private LocalDateTime transactionTime;

    @Column(name = "applied_at", nullable = false)
    private LocalDateTime appliedAt;

    @PrePersist
    void onCreate() {
        this.appliedAt = LocalDateTime.now();
    }
}
