package com.maxcapital.orderprocessing.repository;

import com.maxcapital.orderprocessing.model.Order;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface OrderRepository extends JpaRepository<Order, Long> {

    /**
     * SELECT ... FOR UPDATE sobre la fila de la orden.
     * Segunda linea de defensa ante la ventana de rebalanceo de Kafka:
     * el particionado por numericOrderId ya evita que dos consumidores
     * procesen la misma orden en paralelo en el caso normal, pero este
     * lock blinda contra la ventana breve donde eso podria no cumplirse.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select o from Order o where o.numericOrderId = :numericOrderId")
    Optional<Order> findByIdForUpdate(@Param("numericOrderId") Long numericOrderId);
}
