package com.maxcapital.orderprocessing.dto;

import com.maxcapital.orderprocessing.model.ExecutionLedger;
import com.maxcapital.orderprocessing.model.Order;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record OrderResponse(
    Long numericOrderId,
    String ticker,
    String side,
    String status,
    BigDecimal nominalAmounts,
    BigDecimal leavesNominalAmount,
    BigDecimal accumulativeNominalAmount,
    Integer executionsCount,
    List<LedgerEntryResponse> ledger
) {
    public static OrderResponse from(Order order, List<ExecutionLedger> ledgerEntries) {
        return new OrderResponse(
            order.getNumericOrderId(),
            order.getTicker(),
            order.getSide(),
            order.getStatus().name(),
            order.getNominalAmounts(),
            order.getLeavesNominalAmount(),
            order.getAccumulativeNominalAmount(),
            order.getExecutionsCount(),
            ledgerEntries.stream().map(LedgerEntryResponse::from).toList()
        );
    }

    public record LedgerEntryResponse(
        Long id,
        Long fixId,
        String statusApplied,
        BigDecimal executionPrice,
        BigDecimal executionNominalAmount,
        String secondaryTradeId,
        String operationNumber,
        LocalDateTime appliedAt
    ) {
        public static LedgerEntryResponse from(ExecutionLedger entry) {
            return new LedgerEntryResponse(
                entry.getId(),
                entry.getFixId(),
                entry.getStatusApplied().name(),
                entry.getExecutionPrice(),
                entry.getExecutionNominalAmount(),
                entry.getSecondaryTradeId(),
                entry.getOperationNumber(),
                entry.getAppliedAt()
            );
        }
    }
}
