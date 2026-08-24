package com.maxcapital.orderprocessing.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Representa un ExecutionReport (ER) tal como lo emite el mercado.
 * secondaryTradeId y operationNumber identifica al ER individual (clave de dedup).
 * numericOrderId identifica la orden, se repite en todos sus ER.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ExecutionReport(
    Long fixId,
    Long numericOrderId,
    String marketOrderId,
    String ticker,
    String side,
    String securityType,
    String status,
    BigDecimal orderPrice,
    BigDecimal nominalAmounts,
    BigDecimal leavesNominalAmount,
    BigDecimal accumulativeNominalAmount,
    BigDecimal executionNominalAmount,
    BigDecimal executionPrice,
    BigDecimal avgPrice,
    String secondaryTradeId,
    String operationNumber,
    LocalDateTime transactionTime
) {
}
