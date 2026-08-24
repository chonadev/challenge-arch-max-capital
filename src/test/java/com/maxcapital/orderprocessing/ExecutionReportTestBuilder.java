package com.maxcapital.orderprocessing;

import com.maxcapital.orderprocessing.dto.ExecutionReport;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Helper para construir ExecutionReport de prueba sin repetir los 17
 * campos en cada test. Los valores default son arbitrarios pero
 * consistentes; cada test sobreescribe solo lo que le importa.
 */
public class ExecutionReportTestBuilder {

    private Long fixId = 1L;
    private Long numericOrderId = 9001L;
    private String ticker = "TEST";
    private String side = "BUY";
    private String status = "NEW";
    private BigDecimal nominalAmounts = BigDecimal.valueOf(1000);
    private BigDecimal leavesNominalAmount = BigDecimal.valueOf(1000);
    private BigDecimal accumulativeNominalAmount = BigDecimal.ZERO;
    private BigDecimal executionNominalAmount = BigDecimal.ZERO;
    private BigDecimal executionPrice = null;
    private String secondaryTradeId = "ST-DEFAULT";
    private String operationNumber = "OP-DEFAULT";

    public static ExecutionReportTestBuilder er() {
        return new ExecutionReportTestBuilder();
    }

    public ExecutionReportTestBuilder fixId(Long v) { this.fixId = v; return this; }
    public ExecutionReportTestBuilder numericOrderId(Long v) { this.numericOrderId = v; return this; }
    public ExecutionReportTestBuilder status(String v) { this.status = v; return this; }
    public ExecutionReportTestBuilder leavesNominalAmount(BigDecimal v) { this.leavesNominalAmount = v; return this; }
    public ExecutionReportTestBuilder accumulativeNominalAmount(BigDecimal v) { this.accumulativeNominalAmount = v; return this; }
    public ExecutionReportTestBuilder executionNominalAmount(BigDecimal v) { this.executionNominalAmount = v; return this; }
    public ExecutionReportTestBuilder executionPrice(BigDecimal v) { this.executionPrice = v; return this; }
    public ExecutionReportTestBuilder secondaryTradeId(String v) { this.secondaryTradeId = v; return this; }
    public ExecutionReportTestBuilder operationNumber(String v) { this.operationNumber = v; return this; }

    public ExecutionReport build() {
        return new ExecutionReport(
            fixId, numericOrderId, "M" + numericOrderId, ticker, side, "COMMON_STOCK",
            status, BigDecimal.valueOf(100), nominalAmounts, leavesNominalAmount,
            accumulativeNominalAmount, executionNominalAmount, executionPrice,
            BigDecimal.valueOf(100), secondaryTradeId, operationNumber,
            LocalDateTime.now()
        );
    }
}
