package com.maxcapital.orderprocessing.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    @Column(name = "numeric_order_id")
    private Long numericOrderId;

    @Column(nullable = false, length = 20)
    private String ticker;

    @Column(nullable = false, length = 10)
    private String side;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OrderStatus status;

    @Column(name = "nominal_amounts", nullable = false, precision = 18, scale = 4)
    private BigDecimal nominalAmounts;

    @Column(name = "leaves_nominal_amount", nullable = false, precision = 18, scale = 4)
    private BigDecimal leavesNominalAmount;

    @Column(name = "accumulative_nominal_amount", nullable = false, precision = 18, scale = 4)
    private BigDecimal accumulativeNominalAmount = BigDecimal.ZERO;

    @Column(name = "executions_count", nullable = false)
    private Integer executionsCount = 0;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * Aplica un ER sobre el estado ya persistido de la orden.
     * No sobreescribe: valida la transición y actualiza incrementalmente.
     */
    public void applyExecutionReport(OrderStatus newStatus, BigDecimal leaves,
                                      BigDecimal accumulative) {
        if (!this.status.canTransitionTo(newStatus)) {
            throw new com.maxcapital.orderprocessing.exception.InvalidStateTransitionException(
                "No se puede aplicar ER sobre orden %d en estado terminal %s"
                    .formatted(numericOrderId, this.status));
        }
        this.status = newStatus;
        this.leavesNominalAmount = leaves;
        this.accumulativeNominalAmount = accumulative;
        this.executionsCount = this.executionsCount + 1;
    }
}
