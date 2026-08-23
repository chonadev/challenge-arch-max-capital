package com.maxcapital.orderprocessing.model;

public enum OrderStatus {

    NEW,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED;

    public boolean isTerminal() {
        return this == FILLED || this == CANCELLED;
    }

    /**
     * Valida si es válido pasar de este estado al estado destino.
     * Un ER nunca puede aplicarse sobre una orden ya en estado terminal.
     */
    public boolean canTransitionTo(OrderStatus target) {
        if (this.isTerminal()) {
            return false;
        }
        return true;
    }
}
