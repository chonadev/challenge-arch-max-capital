package com.maxcapital.orderprocessing;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Clase base para tests de integracion. Levanta Postgres y Kafka reales
 * via Testcontainers, compartidos (static) entre todas las clases que
 * la extiendan para no pagar el costo de arrancarlos por clase.
 *
 * Se prueba contra el motor real (no mocks) porque las garantias que
 * este challenge pide (lock pesimista, UNIQUE constraint, ON CONFLICT,
 * SKIP LOCKED) viven en el comportamiento de Postgres, no serian
 * verificables con un repositorio mockeado.
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("orders_test")
            .withUsername("test_user")
            .withPassword("test_pass");

    static final KafkaContainer KAFKA =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    @BeforeAll
    static void startContainers() {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}
